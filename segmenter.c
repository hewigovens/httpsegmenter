/* -*- Mode: c; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*-
   NOTE: the first line of this file sets up source code indentation rules
   for Emacs; it is also a hint to anyone modifying this file.
*/
/* $Id$
 * $HeadURL
 *
 * Copyright (c) 2009 Chase Douglas
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License version 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <float.h>
#include <math.h>

#include "libavformat/avformat.h"

static AVStream *add_output_stream(AVFormatContext *output_format_context, AVStream *input_stream) {
    AVCodecContext *input_codec_context;
    AVCodecContext *output_codec_context;
    AVStream *output_stream;

    output_stream = av_new_stream(output_format_context, 0);
    if (!output_stream) {
        fprintf(stderr, "Could not allocate stream\n");
        exit(1);
    }

    input_codec_context = input_stream->codec;
    output_codec_context = output_stream->codec;

    output_codec_context->codec_id = input_codec_context->codec_id;
    output_codec_context->codec_type = input_codec_context->codec_type;
    output_codec_context->codec_tag = input_codec_context->codec_tag;
    output_codec_context->bit_rate = input_codec_context->bit_rate;
    output_codec_context->extradata = input_codec_context->extradata;
    output_codec_context->extradata_size = input_codec_context->extradata_size;

    if(av_q2d(input_codec_context->time_base) * input_codec_context->ticks_per_frame > av_q2d(input_stream->time_base) && av_q2d(input_stream->time_base) < 1.0/1000) {
        output_codec_context->time_base = input_codec_context->time_base;
        output_codec_context->time_base.num *= input_codec_context->ticks_per_frame;
    }
    else {
        output_codec_context->time_base = input_stream->time_base;
    }

    switch (input_codec_context->codec_type) {
        case CODEC_TYPE_AUDIO:
            output_codec_context->channel_layout = input_codec_context->channel_layout;
            output_codec_context->sample_rate = input_codec_context->sample_rate;
            output_codec_context->channels = input_codec_context->channels;
            output_codec_context->frame_size = input_codec_context->frame_size;
            if ((input_codec_context->block_align == 1 && input_codec_context->codec_id == CODEC_ID_MP3) || input_codec_context->codec_id == CODEC_ID_AC3) {
                output_codec_context->block_align = 0;
            }
            else {
                output_codec_context->block_align = input_codec_context->block_align;
            }
            break;
        case CODEC_TYPE_VIDEO:
            output_codec_context->pix_fmt = input_codec_context->pix_fmt;
            output_codec_context->width = input_codec_context->width;
            output_codec_context->height = input_codec_context->height;
            output_codec_context->has_b_frames = input_codec_context->has_b_frames;

            if (output_format_context->oformat->flags & AVFMT_GLOBALHEADER) {
                output_codec_context->flags |= CODEC_FLAG_GLOBAL_HEADER;
            }
            break;
    default:
        break;
    }

    return output_stream;
}

typedef struct SMSegmentInfo
{
    unsigned int index;
    double duration;
    char * filename;
  
} TSMSegmentInfo;

typedef struct SMPlaylist
{
    /* a ring buffer of segments */
    TSMSegmentInfo * buffer;

    /* maximum number of segments that can be stored in the ring buffer */
    unsigned int bufferCapacity;

    /* index of the first segment on the ring buffer */
    unsigned int first;
    
    /* how many segments are currently in the ring buffer */
    unsigned int count;

    /* shortcuts */
    unsigned int targetDuration;
    char * httpPrefix;

    /* playlist file used for non-live streaming */
    FILE * file;
  
} TSMPlaylist;

static char *
duplicateString(const char * str)
{
    /* unfortunately strdup isn't always available */
    size_t strSize = strlen(str) + 1;
    char * copy = (char *) malloc(strSize);
    memcpy(copy, str, strSize);
    return copy;
}

static TSMPlaylist *
createPlaylist(const unsigned int max_segments,
               const unsigned int target_segment_duration,
               const char * http_prefix)
{
    TSMPlaylist * playlist = (TSMPlaylist *) malloc(sizeof(TSMPlaylist));
    memset(playlist, 0, sizeof(TSMPlaylist));

    if (max_segments)
    {
        playlist->buffer = (TSMSegmentInfo *) malloc(sizeof(TSMSegmentInfo) *
                                                     max_segments);
    }
    
    playlist->bufferCapacity = max_segments;
    playlist->targetDuration = target_segment_duration;
    playlist->httpPrefix = duplicateString(http_prefix);
    
    return playlist;
}

static void
updateLivePlaylist(TSMPlaylist * playlist,
                   const char * playlistFileName,
                   const char * outputFileName,
                   const unsigned int segmentIndex,
                   const double segmentDuration)
{
    unsigned int bufferIndex = 0;
    TSMSegmentInfo * nextSegment = NULL;
    TSMSegmentInfo removeMe;
    memset(&removeMe, 0, sizeof(removeMe));
    assert(!playlist->file);
    
    if (playlist->count == playlist->bufferCapacity)
    {
        /* keep track of the segment that should be removed */
        removeMe = playlist->buffer[playlist->first];
        
        /* make room for the new segment */
        playlist->first++;
        playlist->first %= playlist->bufferCapacity;
    }
    else
    {
        playlist->count++;
    }

    /* store the new segment info */
    bufferIndex = ((playlist->first + playlist->count - 1) %
                   playlist->bufferCapacity);
    nextSegment = &playlist->buffer[bufferIndex];
    nextSegment->filename = duplicateString(outputFileName);
    nextSegment->duration = segmentDuration;
    nextSegment->index = segmentIndex;
    
    /* live streaming -- write full playlist from scratch */
    playlist->file = fopen(playlistFileName, "w+b");
    
    if (playlist->file)
    {
        const TSMSegmentInfo * first = &playlist->buffer[playlist->first];
        
        char tmp[1024] = { 0 };
        snprintf(tmp,
                 sizeof(tmp),
                 "#EXTM3U\n"
                 "#EXT-X-TARGETDURATION:%u\n"
                 "#EXT-X-MEDIA-SEQUENCE:%u\n",
                 playlist->targetDuration,
                 first->index);
        fwrite(tmp, strlen(tmp), 1, playlist->file);
        
        for (unsigned int i = 0; i < playlist->count; i++)
        {
            unsigned int j = ((playlist->first + i) %
                              playlist->bufferCapacity);
            
            const TSMSegmentInfo * segment = &playlist->buffer[j];
            snprintf(tmp,
                     sizeof(tmp),
                     "#EXTINF:%u,\n%s%s\n",
                     (int)(segment->duration + 0.5),
                     playlist->httpPrefix,
                     segment->filename);
            fwrite(tmp, strlen(tmp), 1, playlist->file);
        }
        
        snprintf(tmp, sizeof(tmp), "#EXT-X-ENDLIST\n");
        fwrite(tmp, strlen(tmp), 1, playlist->file);
        
        fclose(playlist->file);
        playlist->file = NULL;
    }
    else
    {
        fprintf(stderr,
                "Could not open m3u8 index file (%s), "
                "no index file will be created\n",
                playlistFileName);
    }
    
    if (removeMe.filename)
    {
        /* remove the oldest segment file */
        remove(removeMe.filename);
        free(removeMe.filename);
    }
}

static void
updatePlaylist(TSMPlaylist * playlist,
               const char * playlistFileName,
               const char * segmentFileName,
               const unsigned int segmentIndex,
               const double segmentDuration)
{
    if (playlist->bufferCapacity > 0)
    {
        /* create a live streaming playlist */
        updateLivePlaylist(playlist,
                           playlistFileName,
                           segmentFileName,
                           segmentIndex,
                           segmentDuration);
    }
    else
    {
        /* append to the existing playlist */
        char tmp[1024] = { 0 };

        if (!playlist->file)
        {
            playlist->file = fopen(playlistFileName, "w+b");
            snprintf(tmp,
                     sizeof(tmp),
                     "#EXTM3U\n"
                     "#EXT-X-TARGETDURATION:%u\n",
                     playlist->targetDuration);
            fwrite(tmp, strlen(tmp), 1, playlist->file);
        }
        
        if (!playlist->file)
        {
            fprintf(stderr,
                    "Could not open m3u8 index file (%s), "
                    "no index file will be created\n",
                    playlistFileName);
        }
        
        snprintf(tmp,
                 sizeof(tmp),
                 "#EXTINF:%u,\n%s%s\n",
                 (int)(segmentDuration + 0.5),
                 playlist->httpPrefix,
                 segmentFileName);
        fwrite(tmp, strlen(tmp), 1, playlist->file);
    }
}

static void
closePlaylist(TSMPlaylist * playlist)
{
    if (playlist->file)
    {
        /* append to the existing playlist */
        char tmp[1024] = { 0 };
        
        snprintf(tmp, sizeof(tmp), "#EXT-X-ENDLIST\n");
        fwrite(tmp, strlen(tmp), 1, playlist->file);
        
        fclose(playlist->file);
        playlist->file = NULL;
    }
}

static void
releasePlaylist(TSMPlaylist ** playlistRef)
{
    TSMPlaylist * playlist = *playlistRef;
    closePlaylist(playlist);
    
    for (unsigned int i = 0; i < playlist->bufferCapacity; i++)
    {
        TSMSegmentInfo * segmentInfo = &playlist->buffer[i];
        if (segmentInfo->filename)
        {
            free(segmentInfo->filename);
        }
    }
    
    free(playlist->buffer);
    free(playlist->httpPrefix);
    free(playlist);
    *playlistRef = NULL;
}
    

typedef struct SMPacketLink
{
    /* packet start time in seconds */
    double timeStamp;

    /* the packet */
    AVPacket packet;

    /* a link to the next packet */
    struct SMPacketLink * next;
    
} TSMPacketLink;

typedef struct SMPacketList
{
    TSMPacketLink * head;
    TSMPacketLink * tail;
    unsigned int size;
} TSMPacketList;

typedef struct SMStreamLace
{
    TSMPacketList ** streams;
    unsigned int numStreams;
} TSMStreamLace;

static TSMPacketLink *
createLink(const AVPacket * packet, double timeStamp)
{
    TSMPacketLink * link = (TSMPacketLink *) malloc(sizeof(TSMPacketLink));
    link->timeStamp = timeStamp;
    link->next = NULL;
    memcpy(&link->packet, packet, sizeof(AVPacket));
    return link;
}

static void
fifoPush(TSMPacketList * packets, const AVPacket * packet, double timeStamp)
{
    TSMPacketLink * link = createLink(packet, timeStamp);
    if (!packets->head)
    {
        assert(!packets->tail);
        assert(!packets->size);
        packets->head = link;
        packets->tail = link;
        packets->size = 1;
    }
    else
    {
        /* attach at the tail */
        assert(packets->size > 0);
        
        packets->tail->next = link;
        packets->tail = link;
        packets->size++;
    }
}

static int
fifoPop(TSMPacketList * packets, AVPacket * packet)
{
    TSMPacketLink * link = packets->head;
    if (!link)
    {
        return 0;
    }
    
    memcpy(packet, &link->packet, sizeof(AVPacket));
    packets->head = link->next;
    packets->size--;
    
    if (!packets->head)
    {
        packets->tail = NULL;
    }
    
    free(link);
    return 1;
}

static TSMPacketList *
createPacketList()
{
    TSMPacketList * packets = (TSMPacketList *)malloc(sizeof(TSMPacketList));
    memset(packets, 0, sizeof(TSMPacketList));
    return packets;
}

static TSMStreamLace *
createStreamLace(unsigned int numStreams)
{
    TSMStreamLace * lace = (TSMStreamLace *)malloc(sizeof(TSMStreamLace));
    lace->streams = (TSMPacketList **)malloc(sizeof(TSMPacketList *) * numStreams);
    
    for (unsigned int i = 0; i < numStreams; i++)
    {
        lace->streams[i] = createPacketList();
    }

    lace->numStreams = numStreams;
    return lace;
}

static void
insertPacket(TSMStreamLace * lace, const AVPacket * packet, double timeStamp)
{
    fifoPush(lace->streams[packet->stream_index], packet, timeStamp);
}

static TSMPacketList *
chooseNextStream(TSMStreamLace * lace)
{
    /* improve lacing so that that audio/video packets that should be
       together do not get stuck into separate segments. */
    
    TSMPacketList * nextStream = NULL;
    double earliestTimeStamp = DBL_MAX;
    for (unsigned int i = 0; i < lace->numStreams; i++)
    {
        TSMPacketList * stream = lace->streams[i];
        if (stream->size && stream->head->timeStamp < earliestTimeStamp)
        {
            nextStream = stream;
            earliestTimeStamp = stream->head->timeStamp;
        }
    }
    
    return nextStream;
}

static int
removePacket(TSMStreamLace * lace, AVPacket * packet)
{
    TSMPacketList * stream = chooseNextStream(lace);
    if (!stream)
    {
        return 0;
    }
    
    return fifoPop(stream, packet);
}

static unsigned int
countPackets(const TSMStreamLace * lace)
{
    unsigned int numPackets = 0;
    for (unsigned int i = 0; i < lace->numStreams; i++)
    {
        const TSMPacketList * stream = lace->streams[i];
        numPackets += stream->size;
    }

    return numPackets;
}

static void
removeAllPackets(TSMStreamLace * lace)
{
    AVPacket packet;
    for (unsigned int i = 0; i < lace->numStreams; i++)
    {
        TSMPacketList * stream = lace->streams[i];
        while (stream->size)
        {
            fifoPop(stream, &packet);
            av_free_packet(&packet);
        }
    }
}

int main(int argc, char **argv)
{
    const char *input = NULL;
    const char *output_prefix = "";
    double target_segment_duration = 0.0;
    char *segment_duration_check = NULL;
    const char *playlist_filename = NULL;
    const char *http_prefix = "";
    long max_tsfiles = 0;
    char *max_tsfiles_check = NULL;
    double prev_segment_time = 0.0;
    double segment_duration = 0.0;
    unsigned int output_index = 0;
    AVOutputFormat *ofmt = NULL;
    AVFormatContext *ic = NULL;
    AVFormatContext *oc = NULL;
    AVStream *video_st = NULL;
    AVStream *audio_st = NULL;
    AVCodec *codec = NULL;
    char *output_filename = NULL;
    int video_index = -1;
    int audio_index = -1;
    int kill_file = 0;
    int decode_done = 0;
    int ret = 0;
    int i = 0;
    FILE * pid_file = NULL;
    TSMStreamLace * streamLace = NULL;
    AVBitStreamFilterContext * vbsf_h264_mp4toannexb = NULL;
    TSMPlaylist * playlist = NULL;
    const double segment_duration_error_tolerance = 0.5;
    
    if (argc < 6 || argc > 8) {
        fprintf(stderr,
                "Usage: %s <input MPEG-TS file> "
                "<segment duration in seconds> "
                "<output MPEG-TS file prefix> "
                "<output m3u8 index file> "
                "<http prefix> "
                "[<segment window size>] "
                "[<search kill file>]\n\n"
                "Compiled by Daniel Espendiller - www.espend.de\n"
                "build on %s %s with %s\n\n"
                "Took some code from:\n"
                " - source:http://svn.assembla.com/svn/legend/segmenter/\n"
                " - iStreamdev:http://projects.vdr-developer.org/git/?p=istreamdev.git;a=tree;f=segmenter;hb=HEAD\n"
                " - live_segmenter:http://github.com/carsonmcdonald/HTTP-Live-Video-Stream-Segmenter-and-Distributor\n",
                argv[0],
                __DATE__,
                __TIME__,
                __VERSION__);
        exit(1);
    }

    // Create PID file
    pid_file=fopen("./segmenter.pid", "wb");
    if (pid_file)
    {
        fprintf(pid_file, "%d", getpid());
        fclose(pid_file);
    }

    av_register_all();

    input = argv[1];
    if (!strcmp(input, "-")) {
        input = "pipe:";
    }
    target_segment_duration = strtod(argv[2], &segment_duration_check);
    if (segment_duration_check == argv[2] || target_segment_duration == HUGE_VAL || target_segment_duration == -HUGE_VAL) {
        fprintf(stderr, "Segment duration time (%s) invalid\n", argv[2]);
        goto error;
    }
    output_prefix = argv[3];
    playlist_filename = argv[4];
    http_prefix=argv[5];
    if (argc >= 7) {
        max_tsfiles = strtol(argv[6], &max_tsfiles_check, 10);
        if (max_tsfiles_check == argv[6] || max_tsfiles < 0 || max_tsfiles >= INT_MAX) {
            fprintf(stderr, "Maximum number of ts files (%s) invalid\n", argv[6]);
            goto error;
        }
    }

    // end programm when it found a file with name 'kill'
    if (argc >= 8) kill_file = atoi(argv[7]);

    output_filename = malloc(sizeof(char) * (strlen(output_prefix) + 15));
    if (!output_filename) {
        fprintf(stderr, "Could not allocate space for output filenames\n");
        goto error;
    }

    playlist = createPlaylist(max_tsfiles,
                              target_segment_duration,
                              http_prefix);
    if (!playlist)
    {
        fprintf(stderr, "Could not allocate space for m3u8 playlist structure\n");
        goto error;
    }

    ret = av_open_input_file(&ic, input, NULL, 0, NULL);
    if (ret != 0) {
        fprintf(stderr, "Could not open input file, make sure it is an mpegts or mp4 file: %d\n", ret);
        goto error;
    }

    if (av_find_stream_info(ic) < 0) {
        fprintf(stderr, "Could not read stream information\n");
        goto error;
    }

    if (strstr(ic->iformat->name, "mp4") != NULL)
    {
        // need to filter the bitstream when re-formatting mp4 to mpeg-ts:
        vbsf_h264_mp4toannexb = av_bitstream_filter_init("h264_mp4toannexb");
    }

#if LIBAVFORMAT_VERSION_MAJOR > 52 || (LIBAVFORMAT_VERSION_MAJOR == 52 && \
                                       LIBAVFORMAT_VERSION_MINOR >= 45)
    ofmt = av_guess_format("mpegts", NULL, NULL);
#else
    ofmt = guess_format("mpegts", NULL, NULL);
#endif
    
    if (!ofmt) {
        fprintf(stderr, "Could not find MPEG-TS muxer\n");
        goto error;
    }

    oc = avformat_alloc_context();
    if (!oc) {
        fprintf(stderr, "Could not allocated output context\n");
        goto error;
    }
    oc->oformat = ofmt;

    video_index = -1;
    audio_index = -1;

    for (i = 0; i < ic->nb_streams && (video_index < 0 || audio_index < 0); i++) {
        switch (ic->streams[i]->codec->codec_type) {
            case CODEC_TYPE_VIDEO:
                video_index = i;
                ic->streams[i]->discard = AVDISCARD_NONE;
                video_st = add_output_stream(oc, ic->streams[i]);
                break;
            case CODEC_TYPE_AUDIO:
                audio_index = i;
                ic->streams[i]->discard = AVDISCARD_NONE;
                audio_st = add_output_stream(oc, ic->streams[i]);
                break;
            default:
                ic->streams[i]->discard = AVDISCARD_ALL;
                break;
        }
    }

    if (av_set_parameters(oc, NULL) < 0) {
        fprintf(stderr, "Invalid output format parameters\n");
        goto error;
    }

    dump_format(oc, 0, output_prefix, 1);
    
    if (video_index >=0) {
      codec = avcodec_find_decoder(video_st->codec->codec_id);
      if (!codec) {
        fprintf(stderr, "Could not find video decoder, key frames will not be honored\n");
      }

      if (avcodec_open(video_st->codec, codec) < 0) {
        fprintf(stderr, "Could not open video decoder, key frames will not be honored\n");
      }
    }

    snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, ++output_index);
    if (url_fopen(&oc->pb, output_filename, URL_WRONLY) < 0) {
        fprintf(stderr, "Could not open '%s'\n", output_filename);
        goto error;
    }

    if (av_write_header(oc)) {
        fprintf(stderr, "Could not write mpegts header to first output file\n");
        goto error;
    }

    prev_segment_time = (double)(ic->start_time) / (double)(AV_TIME_BASE);

    streamLace = createStreamLace(ic->nb_streams);
    
    do {
        double segment_time = 0.0;
        AVPacket packet;
        double packetStartTime = 0.0;
        double packetDuration = 0.0;
        
        if (!decode_done)
        {
            decode_done = av_read_frame(ic, &packet);
            if (!decode_done)
            {
                if (packet.stream_index != video_index &&
                    packet.stream_index != audio_index)
                {
                    av_free_packet(&packet);
                    continue;
                }
                
                double timeStamp = 
                    (double)(packet.pts) * 
                    (double)(ic->streams[packet.stream_index]->time_base.num) /
                    (double)(ic->streams[packet.stream_index]->time_base.den);
                
                if (av_dup_packet(&packet) < 0)
                {
                    fprintf(stderr, "Could not duplicate packet\n");
                    av_free_packet(&packet);
                    break;
                }
                
                insertPacket(streamLace, &packet, timeStamp);
            }
        }
        
        if (countPackets(streamLace) < 50 && !decode_done)
        {
            /* allow the queue to fill up so that the packets can be sorted properly */
            continue;
        }
        
        if (!removePacket(streamLace, &packet))
        {
            if (decode_done)
            {
                /* the queue is empty, we are done */
                break;
            }
            
            assert(decode_done);
            continue;
        }
        
        packetStartTime = 
            (double)(packet.pts) * 
            (double)(ic->streams[packet.stream_index]->time_base.num) /
            (double)(ic->streams[packet.stream_index]->time_base.den);
        
        packetDuration =
            (double)(packet.duration) *
            (double)(ic->streams[packet.stream_index]->time_base.num) /
            (double)(ic->streams[packet.stream_index]->time_base.den);
        
#if !defined(NDEBUG) && (defined(DEBUG) || defined(_DEBUG))
        fprintf(stderr,
                "stream %i, packet [%f, %f)\n",
                packet.stream_index,
                packetStartTime,
                packetStartTime + packetDuration);
#endif

        segment_duration = packetStartTime + packetDuration - prev_segment_time;

        if (packet.stream_index == video_index && (packet.flags & PKT_FLAG_KEY)) {
            segment_time = packetStartTime;
        }
        else if (video_index < 0) {
            segment_time = packetStartTime;
        }
        else {
            segment_time = prev_segment_time;
        }

        if (segment_time - prev_segment_time + segment_duration_error_tolerance > target_segment_duration) 
        {
            put_flush_packet(oc->pb);
            url_fclose(oc->pb);

            updatePlaylist(playlist,
                           playlist_filename,
                           output_filename,
                           output_index,
                           segment_time - prev_segment_time);
            
            snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, ++output_index);
            if (url_fopen(&oc->pb, output_filename, URL_WRONLY) < 0) {
                fprintf(stderr, "Could not open '%s'\n", output_filename);
                break;
            }

            // close when we find the 'kill' file
            if (kill_file) {
                FILE* fp = fopen("kill", "rb");
                if (fp) {
                    fprintf(stderr, "user abort: found kill file\n");
                    fclose(fp);
                    remove("kill");
                    decode_done = 1;
                    removeAllPackets(streamLace);
                }
            }
            prev_segment_time = segment_time;
        }

        if (vbsf_h264_mp4toannexb != NULL &&
            packet.stream_index == video_index)
        {
            AVPacket filteredPacket = packet;
            int a = av_bitstream_filter_filter(vbsf_h264_mp4toannexb,
                                               video_st->codec,
                                               NULL,
                                               &filteredPacket.data,
                                               &filteredPacket.size,
                                               packet.data,
                                               packet.size,
                                               packet.flags & AV_PKT_FLAG_KEY);
            if (a > 0)
            {
                av_free_packet(&packet);
                filteredPacket.destruct = av_destruct_packet;
                packet = filteredPacket;
            }
            else if (a < 0)
            {
                fprintf(stderr,
                        "%s failed for stream %d, codec %s",
                        vbsf_h264_mp4toannexb->filter->name,
                        packet.stream_index,
                        video_st->codec->codec ?
                        video_st->codec->codec->name : "copy");
                av_free_packet(&packet);
                continue;
            }
        }

        ret = av_interleaved_write_frame(oc, &packet);
        if (ret < 0) {
            fprintf(stderr, "Warning: Could not write frame of stream\n");
        }
        else if (ret > 0) {
            fprintf(stderr, "End of stream requested\n");
            av_free_packet(&packet);
            break;
        }

        av_free_packet(&packet);
    } while (!decode_done || countPackets(streamLace) > 0);

    av_write_trailer(oc);

    if (video_index >= 0) {
      avcodec_close(video_st->codec);
    }

    for(i = 0; i < oc->nb_streams; i++) {
        av_freep(&oc->streams[i]->codec);
        av_freep(&oc->streams[i]);
    }

    url_fclose(oc->pb);
    av_free(oc);

    updatePlaylist(playlist,
                   playlist_filename,
                   output_filename,
                   output_index,
                   segment_duration);
    closePlaylist(playlist);
    releasePlaylist(&playlist);
    
    remove("./segmenter.pid");

    return 0;

error:
    remove("./segmenter.pid");

    return 1;

}

// vim:sw=4:tw=4:ts=4:ai:expandtab
