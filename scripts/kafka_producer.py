
import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "jsonl"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
KAFKA_TOPIC = "edo-documents"

DEFAULT_BATCH_SIZE = 1000
DEFAULT_PROGRESS_EVERY = 10000


def find_jsonl_files(dataset_dir: Path) -> list[Path]:
    """Return JSONL files in the selected dataset directory."""
    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory does not exist: {dataset_dir}"
        )

    if not dataset_dir.is_dir():
        raise NotADirectoryError(
            f"Not a directory: {dataset_dir}"
        )

    files = sorted(dataset_dir.glob("events_*.jsonl"))

    if not files:
        raise FileNotFoundError(
            f"No events_*.jsonl files found in: {dataset_dir}"
        )

    return files


def serialize_event(event: dict) -> bytes:
    """Serialize an event as UTF-8 JSON."""
    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def save_checkpoint(
    path: Path,
    dataset_id: str,
    file_index: int,
    line_number: int,
) -> None:
    """Atomically save the last confirmed source position."""
    checkpoint = {
        "dataset_id": dataset_id,
        "file_index": file_index,
        "line_number": line_number,
    }

    temporary_path = path.with_suffix(path.suffix + ".tmp")

    temporary_path.write_text(
        json.dumps(checkpoint, indent=2),
        encoding="utf-8",
    )

    temporary_path.replace(path)


def load_checkpoint(
    path: Path,
    dataset_id: str,
) -> tuple[int, int]:
    """Load checkpoint and verify that it belongs to this dataset."""
    if not path.exists():
        return 0, 0

    data = json.loads(
        path.read_text(encoding="utf-8")
    )

    if data.get("dataset_id") != dataset_id:
        raise ValueError(
            "Checkpoint belongs to a different dataset. "
            "Use the correct checkpoint or start without --resume."
        )

    file_index = data.get("file_index")
    line_number = data.get("line_number")

    if (
        type(file_index) is not int
        or type(line_number) is not int
        or file_index < 0
        or line_number < 0
    ):
        raise ValueError(
            f"Invalid checkpoint: {path}"
        )

    return file_index, line_number


def send_dataset(
    dataset_dir: Path,
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    rate: float | None = None,
    resume: bool = False,
) -> None:
    """Read JSONL files and send events to Kafka."""

    if limit is not None and limit <= 0:
        raise ValueError("--limit must be positive")

    if batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    if progress_every <= 0:
        raise ValueError("--progress-every must be positive")

    if rate is not None and rate <= 0:
        raise ValueError("--rate must be positive")

    jsonl_files = find_jsonl_files(dataset_dir)

    checkpoint_path = dataset_dir / ".kafka_checkpoint.json"
    dataset_id = dataset_dir.resolve().name

    if resume:
        start_file_index, start_line = load_checkpoint(
            checkpoint_path,
            dataset_id,
        )

        if start_file_index > len(jsonl_files):
            raise ValueError(
                "Checkpoint points outside the dataset."
            )

        if (
            start_file_index == len(jsonl_files)
            and start_line != 0
        ):
            raise ValueError(
                "Invalid completed-dataset checkpoint."
            )

        if start_file_index == len(jsonl_files):
            print(
                "Dataset is already fully delivered "
                "according to checkpoint."
            )
            return

    else:
        start_file_index = 0
        start_line = 0

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=serialize_event,
    )

    sent = 0
    errors = 0
    batch = []

    start_time = time.monotonic()
    last_progress = 0
    next_send_time = start_time

    print(f"Dataset: {dataset_dir}")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Files: {len(jsonl_files)}")
    print(f"Resume: {resume}")
    print(
        f"Rate limit: {rate} events/sec"
        if rate is not None
        else "Rate limit: unlimited"
    )
    print("Starting event delivery...\n")

    def deliver_batch() -> None:
        nonlocal sent, errors, last_progress

        for future, file_index, line_number in batch:
            try:
                future.get(timeout=60)

            except KafkaError as exc:
                errors += 1

                raise RuntimeError(
                    f"Kafka delivery failed in "
                    f"{jsonl_files[file_index].name}:"
                    f"{line_number}: {exc}"
                ) from exc

            sent += 1

            save_checkpoint(
                checkpoint_path,
                dataset_id,
                file_index,
                line_number,
            )

            if sent - last_progress >= progress_every:
                elapsed = time.monotonic() - start_time
                speed = sent / elapsed if elapsed > 0 else 0

                print(
                    f"Delivered: {sent:,} events | "
                    f"Speed: {speed:.1f} events/sec | "
                    f"Elapsed: {elapsed:.1f} sec"
                )

                last_progress = sent

        batch.clear()

    try:
        for file_index in range(
            start_file_index,
            len(jsonl_files),
        ):
            file_path = jsonl_files[file_index]

            print(f"Reading: {file_path.name}")

            with file_path.open(
                "r",
                encoding="utf-8",
            ) as file:

                for line_number, line in enumerate(
                    file,
                    start=1,
                ):
                    if (
                        file_index == start_file_index
                        and line_number <= start_line
                    ):
                        continue

                    if (
                        limit is not None
                        and sent + len(batch) >= limit
                    ):
                        break

                    if not line.strip():
                        if batch:
                            deliver_batch()

                        save_checkpoint(
                            checkpoint_path,
                            dataset_id,
                            file_index,
                            line_number,
                        )

                        continue

                    try:
                        event = json.loads(line)

                        if not isinstance(event, dict):
                            raise ValueError(
                                "Event must be a JSON object"
                            )

                        document_id = event.get("DocumentId")

                        if not document_id:
                            raise ValueError(
                                "Event has no DocumentId"
                            )

                        if rate is not None:
                            now = time.monotonic()

                            if now < next_send_time:
                                time.sleep(
                                    next_send_time - now
                                )

                            next_send_time = max(
                                next_send_time + 1 / rate,
                                time.monotonic(),
                            )

                        future = producer.send(
                            KAFKA_TOPIC,
                            key=document_id,
                            value=event,
                        )

                        batch.append(
                            (
                                future,
                                file_index,
                                line_number,
                            )
                        )

                    except (
                        json.JSONDecodeError,
                        ValueError,
                    ) as exc:
                        errors += 1

                        raise RuntimeError(
                            f"Invalid event in "
                            f"{file_path.name}:"
                            f"{line_number}: {exc}"
                        ) from exc

                    if len(batch) >= batch_size:
                        deliver_batch()

                if batch:
                    deliver_batch()

            if (
                limit is not None
                and sent >= limit
            ):
                break

        # Mark the dataset as complete only after all files
        # have been successfully processed.
        if (
            limit is None
            or sent < limit
        ):
            save_checkpoint(
                checkpoint_path,
                dataset_id,
                len(jsonl_files),
                0,
            )

    finally:
        producer.flush(timeout=60)
        producer.close(timeout=60)

    elapsed = time.monotonic() - start_time

    print("\nDelivery summary")
    print("----------------")
    print(f"Successfully sent: {sent:,}")
    print(f"Errors: {errors:,}")
    print(f"Elapsed time: {elapsed:.2f} sec")

    if elapsed > 0:
        print(
            f"Average speed: {sent / elapsed:.1f} events/sec"
        )

    if errors:
        raise RuntimeError(
            f"Delivery completed with {errors} errors"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Send JSONL EDO events to Kafka "
            "with checkpoint support."
        )
    )

    parser.add_argument(
        "dataset",
        help="Dataset directory name inside data/jsonl/",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of successfully sent events.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of events per delivery batch.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print progress after this many events.",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Maximum sending rate in events per second.",
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the saved checkpoint.",
    )

    args = parser.parse_args()

    try:
        send_dataset(
            dataset_dir=DATA_ROOT / args.dataset,
            limit=args.limit,
            batch_size=args.batch_size,
            progress_every=args.progress_every,
            rate=args.rate,
            resume=args.resume,
        )

    except (
        FileNotFoundError,
        NotADirectoryError,
        ValueError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
