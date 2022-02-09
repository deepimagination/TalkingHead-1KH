import argparse
import multiprocessing as mp
import os
from functools import partial
from time import time as timer

from pytube import YouTube
from tqdm import tqdm

import logging
from pathlib import Path
from utils.logger import install_mp_handler, VersionCtrlLogger
from utils.misc import remove_corrupted_videos

parser = argparse.ArgumentParser()
parser.add_argument('--input_list', type=str, required=True,
                    help='List of youtube video ids')
parser.add_argument('--output_dir', type=str, default='data/youtube_videos',
                    help='Location to download videos')
parser.add_argument('--num_workers', type=int, default=8,
                    help='How many multiprocessing workers?')
parser.add_argument('--log_dir', type=str, default='logs/',
                    help='Store download logs for sanity check.')
parser.add_argument('--no_delete_corrupted', action='store_true',
                    help='not delete corruped video (failed to download videos)')
args = parser.parse_args()


def download_video(output_dir, video_id):
    r"""Download video."""
    video_path = '%s/%s.mp4' % (output_dir, video_id)
    if not os.path.isfile(video_path):
        try:
            # Download the highest quality mp4 stream.
            yt = YouTube('https://www.youtube.com/watch?v=%s' % (video_id))
            stream = yt.streams.filter(subtype='mp4', only_video=True, adaptive=True).first()
            if stream is None:
                stream = yt.streams.filter(subtype='mp4').first()
            stream.download(output_path=output_dir, filename=video_id + '.mp4')
            logging.info('Succeed to download %s' % (video_id))
        except Exception as e:
            logging.error(e)
            logging.error('Failed to download %s' % (video_id))
    else:
        logging.info('File exists: %s' % (video_id))


if __name__ == '__main__':
    # Setup logger
    VersionCtrlLogger(log_dir=args.log_dir)
    install_mp_handler()
    
    # Remove corrupted videos
    if not args.no_delete_corrupted:
       remove_corrupted_videos(args.log_dir, args.output_dir, logging.info)

    # Read list of videos.
    video_ids = []
    with open(args.input_list) as fin:
        for line in fin:
            video_ids.append(line.strip())

    # Create output folder.
    os.makedirs(args.output_dir, exist_ok=True)

    # Download videos.
    downloader = partial(download_video, args.output_dir)

    start = timer()
    pool_size = args.num_workers
    print('Using pool size of %d' % (pool_size))
    with mp.Pool(processes=pool_size) as p:
        _ = list(tqdm(p.imap_unordered(downloader, video_ids), total=len(video_ids)))
    print('Elapsed time: %.2f' % (timer() - start))
