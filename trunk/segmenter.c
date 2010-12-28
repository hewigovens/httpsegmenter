/* $Id$
 * $HeadURL$
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

FILE * start_index_file(const char tmp_index[])
{
    FILE * tmp_index_fp = fopen(tmp_index, "w+b");
    if (!tmp_index_fp)
    {
        fprintf(stderr, "Could not open temporary m3u8 index file (%s), no index file will be created\n", tmp_index);
        return NULL;
    };

    return tmp_index_fp;
}

FILE * write_index_file(FILE * tmp_index_fp,
                        const double segment_duration,
                        const char output_prefix[],
                        const char http_prefix[],
                        const unsigned int segment_index)
{
    char write_buf[1024] = { 0 };

    if (!tmp_index_fp)
    {
        return NULL;
    }

    snprintf(write_buf,
             sizeof(write_buf),
             "#EXTINF:%u,\n%s%s-%u.ts\n",
             (int)(segment_duration + 0.5),
             http_prefix,
             output_prefix,
             segment_index);
    
    if (fwrite(write_buf, strlen(write_buf), 1, tmp_index_fp) != 1)
    {
        fprintf(stderr, "Could not write to m3u8 index file, will not continue writing to index file\n");
        fclose(tmp_index_fp);
        return NULL;
    }
    
    return tmp_index_fp;
}

void close_index_file(FILE * tmp_index_fp,
                      const char index[],
                      const unsigned int target_segment_duration,
                      const unsigned int first_segment,
                      const int window)
{
    char write_buf[1024] = { 0 };
    FILE * index_fp = NULL;
    
    if (!tmp_index_fp)
    {
        return;
    }
    
    index_fp = fopen(index, "wb");
    if (!index_fp)
    {
        fprintf(stderr, "Could not open m3u8 index file (%s), no index file will be created\n", index);
        return;
    };

    if (window)
    {
        snprintf(write_buf,
                 sizeof(write_buf),
                 "#EXTM3U\n#EXT-X-TARGETDURATION:%u\n#EXT-X-MEDIA-SEQUENCE:%u\n",
                 target_segment_duration,
                 first_segment);
    }
    else
    {
        snprintf(write_buf,
                 sizeof(write_buf),
                 "#EXTM3U\n#EXT-X-TARGETDURATION:%u\n",
                 target_segment_duration);
    }
    
    if (fwrite(write_buf, strlen(write_buf), 1, index_fp) != 1)
    {
        fprintf(stderr, "Could not write to m3u8 index file, will not continue writing to index file\n");
        fclose(index_fp);
        return;
    }

    /* rewind the temp index file and transfer it's contents into the index file */
    {
        char ch;

        rewind(tmp_index_fp);
        while (fread(&ch, 1, 1, tmp_index_fp) == 1)
        {
            fwrite(&ch, 1, 1, index_fp);
        }
    }
    
    snprintf(write_buf, sizeof(write_buf), "#EXT-X-ENDLIST\n");
    if (fwrite(write_buf, strlen(write_buf), 1, index_fp) != 1)
    {
        fprintf(stderr, "Could not write last file and endlist tag to m3u8 index file\n");
    }

    fclose(index_fp);
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

static TSMPacketLink *
createLink(const AVPacket * packet, double timeStamp)
{
    TSMPacketLink * link = (TSMPacketLink *) malloc(sizeof(TSMPacketLink));
    link->timeStamp = timeStamp;
    link->next = NULL;
    memcpy(&link->packet, packet, sizeof(AVPacket));
    return link;
}

static int
packetsCorrectlySorted(const TSMPacketLink * first,
                    const TSMPacketLink * second)
{
    if (first->packet.stream_index == second->packet.stream_index)
    {
        /* assume that the packets in each stream are already correctly sorted */
        return 1;
    }
    
    if (first->timeStamp < second->timeStamp)
    {
        /* improve lacing so that that audio/video packets that should be
           together do not get stuck into separate segments. */
        return 1;
    }
    
    return 0;
}

static void
insertPacket(TSMPacketList * packets, const AVPacket * packet, double timeStamp)
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
        /* insert link into the list sorted in ascending time stamp order */
        TSMPacketLink * prev = NULL;
        
        assert(packets->size > 0);
        for (TSMPacketLink * i = packets->head; i != NULL; i = i->next)
        {
            if (packetsCorrectlySorted(i, link))
            {
                prev = i;
                continue;
            }
            
            if (i == packets->head)
            {
                packets->head = link;
            }
            else
            {
                prev->next = link;
            }
            
            link->next = i;
            packets->size++;
            return;
        }
        
        /* attach at the tail */
        assert(packets->size > 0);
        
        packets->tail->next = link;
        packets->tail = link;
        packets->size++;
    }
}

static int
removePacket(TSMPacketList * packets, AVPacket * packet)
{
    TSMPacketLink * link = packets->head;
    if (!link)
    {
        return 0;
    }
    
    /* the list is sorted, always remove from the head */
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

int main(int argc, char **argv)
{
    const char *input;
    const char *output_prefix;
    double target_segment_duration;
    char *segment_duration_check;
    const char *index;
    char *tmp_index;
    const char *http_prefix;
    long max_tsfiles = 0;
    char *max_tsfiles_check;
    double prev_segment_time = 0.0;
    double segment_duration = 0.0;
    unsigned int output_index = 1;
    AVInputFormat *ifmt;
    AVOutputFormat *ofmt;
    AVFormatContext *ic = NULL;
    AVFormatContext *oc;
    AVStream *video_st = NULL;
    AVStream *audio_st = NULL;
    AVCodec *codec;
    char *output_filename;
    char *remove_filename;
    int video_index;
    int audio_index;
    int kill_file = 0;
    unsigned int first_segment = 1;
    unsigned int last_segment = 0;
    int decode_done = 0;
    char *dot;
    int ret;
    int i;
    int remove_file;
    FILE * pid_file;
    TSMPacketList * packetQueue = createPacketList();
    FILE * tmp_index_fp = NULL;
    
    if (argc < 6 || argc > 8) {
        fprintf(stderr, "Usage: %s <input MPEG-TS file> <segment duration in seconds> <output MPEG-TS file prefix> <output m3u8 index file> <http prefix> [<segment window size>] [<search kill file>]\n\nCompiled by Daniel Espendiller - www.espend.de\nbuild on %s %s with %s\n\nTook some code from:\n - source:http://svn.assembla.com/svn/legend/segmenter/\n - iStreamdev:http://projects.vdr-developer.org/git/?p=istreamdev.git;a=tree;f=segmenter;hb=HEAD\n - live_segmenter:http://github.com/carsonmcdonald/HTTP-Live-Video-Stream-Segmenter-and-Distributor", argv[0], __DATE__, __TIME__, __VERSION__);
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
    index = argv[4];
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

    remove_filename = malloc(sizeof(char) * (strlen(output_prefix) + 15));
    if (!remove_filename) {
        fprintf(stderr, "Could not allocate space for remove filenames\n");
        goto error;
    }

    output_filename = malloc(sizeof(char) * (strlen(output_prefix) + 15));
    if (!output_filename) {
        fprintf(stderr, "Could not allocate space for output filenames\n");
        goto error;
    }

    tmp_index = malloc(strlen(index) + 2);
    if (!tmp_index) {
        fprintf(stderr, "Could not allocate space for temporary index filename\n");
        goto error;
    }

    strncpy(tmp_index, index, strlen(index) + 2);
    dot = strrchr(tmp_index, '/');
    dot = dot ? dot + 1 : tmp_index;
    for (i = strlen(tmp_index) + 1; i > dot - tmp_index; i--) {
        tmp_index[i] = tmp_index[i - 1];
    }
    *dot = '.';

    ifmt = av_find_input_format("mpegts");
    if (!ifmt) {
        fprintf(stderr, "Could not find MPEG-TS demuxer\n");
        goto error;
    }

    ret = av_open_input_file(&ic, input, ifmt, 0, NULL);
    if (ret != 0) {
        fprintf(stderr, "Could not open input file, make sure it is an mpegts file: %d\n", ret);
        goto error;
    }

    if (av_find_stream_info(ic) < 0) {
        fprintf(stderr, "Could not read stream information\n");
        goto error;
    }

    #if LIBAVFORMAT_VERSION_MAJOR >= 52 && LIBAVFORMAT_VERSION_MINOR >= 45
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
        fprintf(stderr, "Could not allocated output context");
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

    snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, output_index++);
    if (url_fopen(&oc->pb, output_filename, URL_WRONLY) < 0) {
        fprintf(stderr, "Could not open '%s'\n", output_filename);
        goto error;
    }

    if (av_write_header(oc)) {
        fprintf(stderr, "Could not write mpegts header to first output file\n");
        goto error;
    }

    prev_segment_time = (double)(ic->start_time) / (double)(AV_TIME_BASE);

    tmp_index_fp = start_index_file(tmp_index);

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
                double timeStamp = 
                    (double)(packet.pts) * 
                    (double)(ic->streams[packet.stream_index]->time_base.num) /
                    (double)(ic->streams[packet.stream_index]->time_base.den);
                 
                if (av_dup_packet(&packet) < 0)
                {
                    fprintf(stderr, "Could not duplicate packet");
                    av_free_packet(&packet);
                    break;
                }
                
                insertPacket(packetQueue, &packet, timeStamp);
            }
        }
        
        if (packetQueue->size < 50 && !decode_done)
        {
            /* allow the queue to fill up so that the packets can be sorted properly */
            continue;
        }
        
        if (!removePacket(packetQueue, &packet))
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

        if (segment_time - prev_segment_time >= target_segment_duration) {
            put_flush_packet(oc->pb);
            url_fclose(oc->pb);

            if (max_tsfiles && (int)(last_segment - first_segment) >= max_tsfiles - 1) {
                remove_file = 1;
                first_segment++;
            }
            else {
                remove_file = 0;
            }

            tmp_index_fp = write_index_file(tmp_index_fp,
                                            segment_time - prev_segment_time,
                                            output_prefix,
                                            http_prefix,
                                            ++last_segment);
            
            if (remove_file) {
                snprintf(remove_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, first_segment - 1);
                remove(remove_filename);
            }

            snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, output_index++);
            if (url_fopen(&oc->pb, output_filename, URL_WRONLY) < 0) {
                fprintf(stderr, "Could not open '%s'\n", output_filename);
                break;
            }

            // close when when we find the file 'kill'
            if (kill_file) {
                FILE* fp = fopen("kill", "rb");
                if (fp) {
                    fprintf(stderr, "user abort: found kill file\n");		  
                    fclose(fp);
                    remove("kill");
                    decode_done = 1;
                    packetQueue->size = 0;
                }
            }
            prev_segment_time = segment_time;
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
    } while (!decode_done || packetQueue->size);

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

    if (max_tsfiles && (int)(last_segment - first_segment) >= max_tsfiles - 1) {
        remove_file = 1;
        first_segment++;
    }
    else {
        remove_file = 0;
    }

    tmp_index_fp = write_index_file(tmp_index_fp, segment_duration, output_prefix, http_prefix, ++last_segment);
    close_index_file(tmp_index_fp, index, target_segment_duration, first_segment, max_tsfiles);
    tmp_index_fp = NULL;
    
    if (remove_file) {
        snprintf(remove_filename, strlen(output_prefix) + 15, "%s-%u.ts", output_prefix, first_segment - 1);
        remove(remove_filename);
    }

    remove("./segmenter.pid");

    return 0;

error:
    remove("./segmenter.pid");

    return 1;

}

// vim:sw=4:tw=4:ts=4:ai:expandtab
