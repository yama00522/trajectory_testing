import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn


class CsvMerger:
    def __init__(
        self,
        input_dir: str | Path,
        output_dir: str | Path,
        num_files: int = 20,
        interval_sec: int = 5,
    ) -> None:
        self.input_dir: Path = Path(input_dir)
        self.output_dir: Path = Path(output_dir)
        self.num_files: int = num_files
        self.interval_sec: int = interval_sec

        self.output_dir.mkdir(parents=True, exist_ok=True)

        now: datetime = datetime.now()
        self.next_tick: datetime = now.replace(microsecond=0) + timedelta(seconds=1)

        # ★ ポインタファイル
        self.latest_path: Path = self.output_dir / "latest.txt"

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

        # ★ ファイル名は「実時間」
        now: datetime = datetime.now()
        timestamp: str = now.strftime("%Y%m%d_%H%M%S")

        merged_path: Path = self.output_dir / f"merged_{timestamp}.csv"

        # ① 完成物を書き出す
        with open(merged_path, mode="w", newline="", encoding="utf-8") as fout:
            writer = csv.writer(fout)
            header_written: bool = False

            for csv_file in target_files:
                with open(csv_file, mode="r", encoding="utf-8") as fin:
                    reader = csv.reader(fin)
                    header = next(reader)

                    if not header_written:
                        writer.writerow(header)
                        header_written = True

                    for row in reader:
                        writer.writerow(row)

        # ② 最後にポインタ更新（これが atomic 相当）
        self.latest_path.write_text(merged_path.name, encoding="utf-8")

        print(f"Merged -> {merged_path} (latest updated)")

    def run(self) -> NoReturn:
        while True:
            self._sleep_until_next_tick()
            self._merge_csvs()
            self.next_tick += timedelta(seconds=self.interval_sec)
