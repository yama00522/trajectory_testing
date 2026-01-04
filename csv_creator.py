import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn


class CsvCreator:
    def __init__(self, output_dir: str | Path, interval_sec: int = 1) -> None:
        self.output_dir: Path = Path(output_dir)
        self.interval_sec: int = interval_sec

        self.output_dir.mkdir(parents=True, exist_ok=True)

        now: datetime = datetime.now()
        self.next_tick: datetime = now.replace(microsecond=0) + timedelta(seconds=1)

    def _sleep_until_next_tick(self) -> None:
        sleep_time: float = (self.next_tick - datetime.now()).total_seconds()
        if sleep_time > 0:
            time.sleep(sleep_time)

    # def _create_csv(self) -> None:
    #     timestamp: str = self.next_tick.strftime("%Y%m%d_%H%M%S")
    #     filepath: Path = self.output_dir / f"{timestamp}.csv"

    #     with open(filepath, mode="w", newline="", encoding="utf-8") as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["column1", "column2"])
    #         writer.writerow([f"{timestamp}_1_1", f"{timestamp}_1_2"])
    #         writer.writerow([f"{timestamp}_2_1", f"{timestamp}_2_2"])
    #         writer.writerow([f"{timestamp}_3_1", f"{timestamp}_3_2"])

    #     print(f"Created: {filepath}")
    
    def _create_csv(self) -> None:
        timestamp: str = self.next_tick.strftime("%Y%m%d_%H%M%S")
        filepath: Path = self.output_dir / f"{timestamp}.csv"

        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # ヘッダー
            writer.writerow(["column1", "column2"])

            # データ行（1000行）
            for i in range(1, 1001):
                writer.writerow([
                    f"{timestamp}_{i}_1",
                    f"{timestamp}_{i}_2"
                ])

        print(f"Created: {filepath}")

    def run(self) -> NoReturn:
        while True:
            self._sleep_until_next_tick()
            self._create_csv()
            self.next_tick += timedelta(seconds=self.interval_sec)
