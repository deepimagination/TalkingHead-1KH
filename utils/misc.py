from pathlib import Path
import os
import tempfile


def load_download_logs(log_dir, output_dir, print_func=print):
    if not isinstance(log_dir, Path):
        log_dir = Path(log_dir)
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    succeed_video_id_f = tempfile.NamedTemporaryFile("r")
    failed_video_id_f = tempfile.NamedTemporaryFile("r")

    for path in log_dir.glob("version_*/*.log"):
        os.system(
            f"cat {path} | grep 'Succeed to download' >> {succeed_video_id_f.name}"
        )
        os.system(f"cat {path} | grep 'Failed to download' >> {failed_video_id_f.name}")
        print_func(f"load log: {path}")

    succeed_video_id_ls = []
    failed_video_id_ls = []
    downloaded_video_id_ls = []

    for line in succeed_video_id_f.readlines():
        succeed_video_id_ls.append(line.strip().split(" ")[-1])
    for line in failed_video_id_f.readlines():
        failed_video_id_ls.append(line.strip().split(" ")[-1])
    for line in output_dir.glob("*.mp4"):
        downloaded_video_id_ls.append(line.stem.strip())

    succeed_video_id_f.close()
    failed_video_id_f.close()

    succeed_video_id_set = set(succeed_video_id_ls)
    failed_video_id_set = set(failed_video_id_ls)
    downloaded_video_id_set = set(downloaded_video_id_ls)

    incomplete_video_id_set = downloaded_video_id_set - succeed_video_id_set
    failed_video_id_set = failed_video_id_set - succeed_video_id_set

    print_func(
        f"number of succeed videos: {len(succeed_video_id_set)}"
        + f" (before uniq: {len(succeed_video_id_ls)})",
    )
    print_func(
        f"number of failed videos: {len(failed_video_id_set)}"
        + f" (before uniq and clean: {len(failed_video_id_ls)})",
    )
    print_func(f"number of incomplete videos: {len(incomplete_video_id_set)}")

    return succeed_video_id_set, failed_video_id_set, incomplete_video_id_set


def remove_corrupted_videos(log_dir, output_dir, print_func=print):
    succeed_video_ids, failed_video_ids, incomplete_video_id_set = load_download_logs(
        log_dir, output_dir, print_func
    )

    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    for failed_video_name in failed_video_ids | incomplete_video_id_set:
        failed_video_path = output_dir.joinpath(failed_video_name + ".mp4")
        if failed_video_path.exists():
            failed_video_path.unlink()
            print_func(f"Remove corrupted video: {failed_video_path}")

    return None


if __name__ == "__main__":
    sset, fset, iset = load_download_logs("logs", "train/raw_videos")

    from IPython.core.debugger import set_trace

    set_trace()
