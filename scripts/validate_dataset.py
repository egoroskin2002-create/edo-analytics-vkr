
import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "jsonl"

EXPECTED_FIELDS = {
    "EventTime",
    "EventId",
    "DocumentId",
    "RegionId",
    "BranchId",
    "SourceBranchId",
    "TargetBranchId",
    "EventType",
    "DocumentType",
    "PayloadSize",
    "DurationMs",
}

INITIAL_EVENTS = [
    "DocumentCreated",
    "DocumentSent",
    "DocumentReceived",
]


def validate_dataset(dataset_dir: Path) -> bool:
    total_events = 0
    total_documents = 0
    errors = 0
    signed = 0
    rejected = 0

    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_dir}")

    files = sorted(dataset_dir.glob("events_*.jsonl"))

    if not files:
        raise FileNotFoundError(
            f"No events_*.jsonl files found in {dataset_dir}"
        )

    print(f"Dataset: {dataset_dir}")
    print(f"Files: {len(files)}\n")

    for path in files:
        file_events = 0
        group = []

        with path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    event = json.loads(line)

                    if not isinstance(event, dict):
                        raise ValueError("Event must be a JSON object")

                    if set(event) != EXPECTED_FIELDS:
                        missing = EXPECTED_FIELDS - set(event)
                        extra = set(event) - EXPECTED_FIELDS
                        raise ValueError(
                            f"Incorrect fields. Missing: {missing}; Extra: {extra}"
                        )

                    group.append(event)
                    file_events += 1
                    total_events += 1

                    if len(group) == 4:
                        types = [item["EventType"] for item in group]
                        document_ids = [
                            item["DocumentId"] for item in group
                        ]
                        event_ids = [
                            item["EventId"] for item in group
                        ]
                        routes = [
                            (
                                item["SourceBranchId"],
                                item["TargetBranchId"],
                            )
                            for item in group
                        ]

                        if (
                            types[:3] != INITIAL_EVENTS
                            or types[3] not in (
                                "DocumentSigned",
                                "DocumentRejected",
                            )
                            or len(set(document_ids)) != 1
                            or len(set(event_ids)) != 4
                            or len(set(routes)) != 1
                        ):
                            raise ValueError(
                                "Invalid document lifecycle"
                            )

                        if types[3] == "DocumentSigned":
                            signed += 1
                        else:
                            rejected += 1

                        total_documents += 1
                        group.clear()

                except (json.JSONDecodeError, ValueError) as exc:
                    errors += 1
                    print(
                        f"Error in {path.name}:{line_number}: {exc}"
                    )
                    group.clear()

        if group:
            errors += 1
            print(
                f"Error: incomplete document lifecycle in {path.name}"
            )

        print(
            f"{path.name}: {file_events:,} events checked"
        )

    print("\nVALIDATION SUMMARY")
    print("------------------")
    print(f"Total events: {total_events:,}")
    print(f"Total documents: {total_documents:,}")
    print(f"Signed: {signed:,}")
    print(f"Rejected: {rejected:,}")
    print(f"Errors: {errors}")

    if errors == 0:
        print("\nDataset validation passed.")
        return True

    print("\nDataset validation failed.")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Validate a generated EDO JSONL dataset."
    )

    parser.add_argument(
        "dataset",
        help="Dataset directory name inside data/jsonl/",
    )

    args = parser.parse_args()
    dataset_dir = DATA_ROOT / args.dataset

    try:
        success = validate_dataset(dataset_dir)
    except FileNotFoundError as exc:
        parser.error(str(exc))

    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
