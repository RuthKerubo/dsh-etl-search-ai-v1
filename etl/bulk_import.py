"""Bulk import datasets from various sources"""
import json
import csv
from typing import List, Dict, Any
from datetime import datetime
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from etl.validation.iso_compliance import check_compliance
import os


class BulkImporter:
    def __init__(self, mongodb_uri: str = None, database: str = "dsh_etl_search"):
        uri = mongodb_uri or os.getenv('MONGODB_URI')
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db['datasets']
        print("Loading embedding model...")
        self.model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        print("Model loaded")

    def import_from_json(self, filepath: str, source_name: str) -> Dict[str, Any]:
        """Import datasets from a JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)

        datasets = data if isinstance(data, list) else data.get('datasets', [data])
        return self._import_datasets(datasets, source_name)

    def import_from_csv(self, filepath: str, source_name: str) -> Dict[str, Any]:
        """Import datasets from a CSV file"""
        datasets = []
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert keywords string to list if needed
                if 'keywords' in row and isinstance(row['keywords'], str):
                    row['keywords'] = [k.strip() for k in row['keywords'].split(',')]
                datasets.append(row)
        return self._import_datasets(datasets, source_name)

    def _import_datasets(self, datasets: List[Dict], source_name: str) -> Dict[str, Any]:
        """Process and import datasets"""
        results = {
            "source": source_name,
            "total": len(datasets),
            "imported": 0,
            "skipped": 0,
            "errors": []
        }

        for i, dataset in enumerate(datasets):
            try:
                # Ensure identifier
                if not dataset.get('identifier'):
                    dataset['identifier'] = f"{source_name}-{i+1:04d}"

                # Skip if no title
                if not dataset.get('title'):
                    results['skipped'] += 1
                    results['errors'].append({"index": i, "error": "Missing title"})
                    continue

                # Check if exists
                if self.collection.find_one({"identifier": dataset['identifier']}):
                    results['skipped'] += 1
                    continue

                # Generate embedding
                text = f"{dataset.get('title', '')} {dataset.get('abstract', '')}"
                dataset['embedding'] = self.model.encode(text).tolist()

                # Add metadata
                dataset['source'] = source_name
                dataset['imported_at'] = datetime.utcnow().isoformat()

                # Check compliance
                compliance = check_compliance(dataset)
                dataset['iso_compliance'] = compliance

                # Insert
                self.collection.insert_one(dataset)
                results['imported'] += 1

                if (i + 1) % 10 == 0:
                    print(f"  Processed {i + 1}/{len(datasets)}...")

            except Exception as e:
                results['errors'].append({"index": i, "error": str(e)})

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get current database stats"""
        total = self.collection.count_documents({})
        by_source = {}

        pipeline = [
            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
        ]
        for doc in self.collection.aggregate(pipeline):
            source = doc['_id'] or 'CEH-original'
            by_source[source] = doc['count']

        return {
            "total_datasets": total,
            "by_source": by_source
        }

    def close(self):
        self.client.close()


def main():
    """CLI for bulk import"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description='Bulk import datasets')
    parser.add_argument('filepath', help='Path to JSON or CSV file')
    parser.add_argument('--source', '-s', required=True, help='Source name')
    parser.add_argument('--format', '-f', choices=['json', 'csv'], default='json')

    args = parser.parse_args()

    importer = BulkImporter()

    print(f"Importing from {args.filepath}...")

    if args.format == 'csv' or args.filepath.endswith('.csv'):
        result = importer.import_from_csv(args.filepath, args.source)
    else:
        result = importer.import_from_json(args.filepath, args.source)

    print(f"\nImport Results:")
    print(f"   Source: {result['source']}")
    print(f"   Total: {result['total']}")
    print(f"   Imported: {result['imported']}")
    print(f"   Skipped: {result['skipped']}")
    if result['errors']:
        print(f"   Errors: {len(result['errors'])}")

    stats = importer.get_stats()
    print(f"\nDatabase Stats:")
    print(f"   Total datasets: {stats['total_datasets']}")
    for source, count in stats['by_source'].items():
        print(f"   - {source}: {count}")

    importer.close()


if __name__ == "__main__":
    main()
