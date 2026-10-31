
"""Generate synthetic EDO document lifecycle events in JSONL format."""

from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "jsonl"

# Dataset size: 10 GiB (10 * 1024**3 bytes)
TARGET_DATASET_BYTES = 10 * 1024**3

# Target size of an individual JSONL file: 500 MiB
TARGET_FILE_BYTES = 500 * 1024**2

# Event generation period
START_DATE = "2022-01-01T00:00:00"
END_DATE = "2025-12-31T23:59:59"

# Document outcome probabilities
SIGNED_PROBABILITY = 0.78
REJECTED_PROBABILITY = 0.22

# Weighted document type distribution
DOCUMENT_TYPE_WEIGHTS = {
    "Invoice": 0.40,
    "Contract": 0.25,
    "Act": 0.20,
    "Waybill": 0.15,
}

# Regions and their branches
REGIONS = {
    "West": ["Branch_1_west", "Branch_2_west", "Branch_3_west"],
    "East": ["Branch_1_east", "Branch_2_east", "Branch_3_east"],
    "North": ["Branch_1_north", "Branch_2_north", "Branch_3_north"],
}

# Event names in a document lifecycle
INITIAL_EVENTS = [
    "DocumentCreated",
    "DocumentSent",
    "DocumentReceived",
]
FINAL_EVENTS = ["DocumentSigned", "DocumentRejected"]

# JSONL output configuration
OUTPUT_PREFIX = "events"


import argparse
import json
import random
import uuid
from datetime import datetime, timedelta

def random_creation_time() -> datetime:
    """Choose a creation time with lower activity during night hours."""
    start = datetime.fromisoformat(START_DATE)
    end = datetime.fromisoformat(END_DATE)

    # Reserve up to 9 days for the three lifecycle transitions.
    latest_creation = end - timedelta(days=9)

    # Weighted activity by time of day.
    time_ranges = [
        (0, 5, 0.03),    # 00:00–05:59
        (6, 8, 0.07),    # 06:00–08:59
        (9, 11, 0.25),   # 09:00–11:59
        (12, 14, 0.20),  # 12:00–14:59
        (15, 17, 0.30),  # 15:00–17:59
        (18, 23, 0.15),  # 18:00–23:59
    ]

    while True:
        day_offset = random.randint(
            0, (latest_creation.date() - start.date()).days
        )
        day = start + timedelta(days=day_offset)

        hour_start, hour_end, _ = random.choices(
            time_ranges,
            weights=[item[2] for item in time_ranges],
            k=1,
        )[0]

        hour = random.randint(hour_start, hour_end)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        created_at = day.replace(
            hour=hour,
            minute=minute,
            second=second,
        )

        if start <= created_at <= latest_creation:
            return created_at

def random_payload_size() -> int:
    """Choose a document size using a weighted distribution."""
    ranges = [
        (1 * 1024, 100 * 1024, 0.60),
        (100 * 1024, 1 * 1024 * 1024, 0.30),
        (1 * 1024 * 1024, 10 * 1024 * 1024, 0.09),
        (10 * 1024 * 1024, 50 * 1024 * 1024, 0.01),
    ]

    selected = random.choices(
        ranges,
        weights=[item[2] for item in ranges],
        k=1,
    )[0]

    return random.randint(selected[0], selected[1])


def choose_branch() -> tuple[str, str]:
    """Return a randomly selected region and branch."""
    region = random.choice(list(REGIONS.keys()))
    branch = random.choice(REGIONS[region])
    return region, branch


def random_stage_delay() -> int:
    """Return a positive delay between lifecycle events, in milliseconds."""
    # Most stages finish within hours; longer delays are less common.
    delay_hours = random.choices(
        population=[1, 2, 4, 8, 24, 48, 72],
        weights=[10, 20, 25, 20, 15, 7, 3],
        k=1,
    )[0]

    # Add variation within the selected number of hours.
    delay_seconds = random.randint(
        max(1, delay_hours * 1800),
        delay_hours * 3600,
    )
    return delay_seconds * 1000


def generate_document_events() -> list[dict]:
    """Generate all four events for one document."""
    document_id = str(uuid.uuid4())
    document_type = random.choices(
        population=list(DOCUMENT_TYPE_WEIGHTS.keys()),
        weights=list(DOCUMENT_TYPE_WEIGHTS.values()),
        k=1,
    )[0]

    source_region, source_branch = choose_branch()
    target_region, target_branch = choose_branch()

    # Keep the source and target branches stable throughout the lifecycle.
    payload_size = random_payload_size()
    created_at = random_creation_time()

    outcome = random.choices(
        population=FINAL_EVENTS,
        weights=[SIGNED_PROBABILITY, REJECTED_PROBABILITY],
        k=1,
    )[0]

    event_types = INITIAL_EVENTS + [outcome]
    events = []
    event_time = created_at

    for index, event_type in enumerate(event_types):
        if index > 0:
            event_time += timedelta(milliseconds=random_stage_delay())

        if event_type in ("DocumentCreated", "DocumentSent"):
            event_region, event_branch = source_region, source_branch
        else:
            event_region, event_branch = target_region, target_branch

        events.append(
            {
                "EventTime": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "EventId": str(uuid.uuid4()),
                "DocumentId": document_id,
                "RegionId": event_region,
                "BranchId": event_branch,
                "SourceBranchId": source_branch,
                "TargetBranchId": target_branch,
                "EventType": event_type,
                "DocumentType": document_type,
                "PayloadSize": payload_size,
                "DurationMs": (
                    0 if index == 0
                    else int((event_time - previous_time).total_seconds() * 1000)
                ),
            }
        )

        previous_time = event_time

    # Do not allow the final event to fall outside the selected period.
    if events[-1]["EventTime"] > END_DATE.replace("T", " "):
        return generate_document_events()

    return events

def write_dataset(
    target_bytes: int = TARGET_DATASET_BYTES,
    target_file_bytes: int = TARGET_FILE_BYTES,
) -> None:
    """Generate a fresh JSONL dataset without splitting document lifecycles."""
    from datetime import datetime
    from pathlib import Path

    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")
    if target_file_bytes <= 0:
        raise ValueError("target_file_bytes must be positive")

    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    dataset_dir = DATA_ROOT / f"dataset_{run_id}"
    dataset_dir.mkdir(parents=True, exist_ok=False)

    total_bytes = 0
    total_events = 0
    total_documents = 0
    signed_documents = 0
    rejected_documents = 0
    file_number = 1
    file_bytes = 0
    output_file = None

    try:
        while total_bytes < target_bytes:
            events = generate_document_events()

            # Serialize the complete lifecycle before choosing the output file.
            lines = [
                (json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
                for event in events
            ]
            document_data = "".join(lines).encode("utf-8")
            document_bytes = len(document_data)

            # Rotate before writing, never in the middle of a document lifecycle.
            if output_file is None or (
                file_bytes > 0
                and file_bytes + document_bytes > target_file_bytes
            ):
                if output_file is not None:
                    output_file.close()

                file_path = dataset_dir / f"{OUTPUT_PREFIX}_{file_number:04d}.jsonl"
                output_file = file_path.open("wb")
                file_number += 1
                file_bytes = 0

            output_file.write(document_data)
            file_bytes += document_bytes
            total_bytes += document_bytes
            total_events += len(events)
            total_documents += 1

            if events[-1]["EventType"] == "DocumentSigned":
                signed_documents += 1
            else:
                rejected_documents += 1

            if total_documents % 100_000 == 0:
                print(
                    f"Документов: {total_documents:,}; "
                    f"событий: {total_events:,}; "
                    f"записано: {total_bytes / 1024**3:.2f} GiB",
                    flush=True,
                )
    finally:
        if output_file is not None:
            output_file.close()

    print("\nГенерация завершена.")
    print(f"Каталог: {dataset_dir}")
    print(f"Файлов: {file_number - 1}")
    print(f"Документов: {total_documents:,}")
    print(f"Событий: {total_events:,}")
    print(f"Объём: {total_bytes:,} байт ({total_bytes / 1024**3:.3f} GiB)")
    print(f"Подписано: {signed_documents:,}")
    print(f"Отклонено: {rejected_documents:,}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic EDO events in JSONL format."
    )
    parser.add_argument(
        "--size-mib",
        type=int,
        default=10 * 1024,
        help="Target dataset size in MiB (default: 10240 MiB = 10 GiB).",
    )
    args = parser.parse_args()

    if args.size_mib <= 0:
        parser.error("--size-mib must be a positive integer")

    write_dataset(target_bytes=args.size_mib * 1024 * 1024)