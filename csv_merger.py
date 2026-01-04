import csv
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn


class CsvMerger:
    def __init__(
        self,
        input_dir: str | Path,
        output_dir: str | Path,
        outdate_dir: str | Path,
        num_files: int = 20,
        interval_sec: int = 1,
    ) -> None:
        self.input_dir: Path = Path(input_dir)
        self.output_dir: Path = Path(output_dir)
        self.outdate_dir: Path = Path(outdate_dir)
        self.num_files: int = num_files
        self.interval_sec: int = interval_sec

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.outdate_dir.mkdir(parents=True, exist_ok=True)

        now: datetime = datetime.now()
        self.next_tick: datetime = now.replace(microsecond=0) + timedelta(seconds=1)

    def _sleep_until_next_tick(self) -> None:
        sleep_time: float = (self.next_tick - datetime.now()).total_seconds()
        if sleep_time > 0:
            time.sleep(sleep_time)

    def _merge_csvs(self) -> None:
        csv_files: list[Path] = sorted(self.input_dir.glob("*.csv"))
        target_files: list[Path] = csv_files[-self.num_files:]

        if not target_files:
            print("No CSV files found.")
            return

        timestamp: str = self.next_tick.strftime("%Y%m%d_%H%M%S")

        history_path: Path = self.output_dir / f"merged_{timestamp}.csv"

        # outdate は一時ファイル経由
        latest_path: Path = self.outdate_dir / "merged_outdate.csv"
        tmp_path: Path = self.outdate_dir / "merged_outdate.tmp.csv"

        with (
            open(history_path, mode="w", newline="", encoding="utf-8") as fh,
            open(tmp_path, mode="w", newline="", encoding="utf-8") as fl,
        ):
            writer_h = csv.writer(fh)
            writer_l = csv.writer(fl)

            header_written: bool = False

            for csv_file in target_files:
                with open(csv_file, mode="r", encoding="utf-8") as fin:
                    reader = csv.reader(fin)
                    header = next(reader)

                    if not header_written:
                        writer_h.writerow(header)
                        writer_l.writerow(header)
                        header_written = True

                    for row in reader:
                        writer_h.writerow(row)
                        writer_l.writerow(row)

        # 原子的に差し替え（Windows / Linux 両対応）
        os.replace(tmp_path, latest_path)

        print(
            f"Merged {len(target_files)} files -> "
            f"{history_path}, {latest_path}"
        )

    def run(self) -> NoReturn:
        while True:
            self._sleep_until_next_tick()
            self._merge_csvs()
            self.next_tick += timedelta(seconds=self.interval_sec)
