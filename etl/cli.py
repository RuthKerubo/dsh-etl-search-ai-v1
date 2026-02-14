#!/usr/bin/env python3
"""
DSH ETL Search AI - Command Line Interface

Usage:
    python -m etl.cli init                    # Create MongoDB indexes
    python -m etl.cli run                     # Run ETL pipeline (fetch, parse, store)
    python -m etl.cli embed                   # Generate embeddings
    python -m etl.cli status                  # Show current status
    python -m etl.cli search "query"          # Test search
"""

import asyncio
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
from datetime import timedelta
load_dotenv()


# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def load_dataset_ids(filepath: str = "data/metadata-file-identifiers.txt") -> list[str]:
    """Load dataset IDs from file."""
    path = Path(filepath)
    if not path.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    ids = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    print(f"Loaded {len(ids)} dataset IDs from {filepath}")
    return ids


def cmd_init():
    """Initialize MongoDB indexes."""
    from etl.repository import MongoDBConnection

    async def run():
        conn = MongoDBConnection()
        await conn.connect()
        await conn.create_indexes()
        await conn.close()
        print("MongoDB indexes created successfully")
        print("   - Text index on title + abstract")
        print("   - Unique index on identifier")
        print()
        print("NOTE: Vector search index must be created via Atlas UI/API:")
        print('   Index name: "vector_index"')
        print("   Field: embedding (vector, 384 dimensions, cosine)")

    asyncio.run(run())


def cmd_run(ids_file: str = "data/metadata-file-identifiers.txt"):
    """Run the ETL pipeline."""
    from etl.client import CEHCatalogueClient
    from etl.parsers import get_default_registry
    from etl.repository import MongoDBConnection, DatasetRepository
    from etl.pipeline import ETLPipeline, PipelineConfig, create_console_progress

    # Load IDs
    dataset_ids = load_dataset_ids(ids_file)

    cache_dir = os.getenv("CACHE_DIR", "data/cache")
    print(f"Cache: {cache_dir}")
    print()

    async def run_pipeline():
        conn = MongoDBConnection()
        await conn.connect()

        repo = DatasetRepository(conn.datasets)

        client = CEHCatalogueClient(
            cache_dir=cache_dir,
            cache_ttl=timedelta(hours=24),
            concurrency=3,
            request_delay=0.3,
        )

        pipeline = ETLPipeline(
            client=client,
            parser_registry=get_default_registry(),
            repository=repo,
            config=PipelineConfig(batch_size=20),
        )

        print("Starting ETL pipeline...")
        print()

        result = await pipeline.run(
            dataset_ids,
            progress_callback=create_console_progress(),
        )

        await conn.close()
        return result

    result = asyncio.run(run_pipeline())

    print()
    print(result.summary())

    if result.failed:
        print()
        print("Failed datasets:")
        for f in result.failed[:10]:
            print(f"   {f.dataset_id}: {f.error_message}")
        if len(result.failed) > 10:
            print(f"   ... and {len(result.failed) - 10} more")

    return result


def cmd_embed():
    """Generate embeddings for all datasets."""
    from etl.embeddings import SentenceTransformerService, VectorStore, create_indexing_progress
    from etl.repository import MongoDBConnection, DatasetRepository

    async def run():
        conn = MongoDBConnection()
        await conn.connect()

        repo = DatasetRepository(conn.datasets)
        datasets = await repo.get_all_for_embedding()

        if not datasets:
            print("No datasets in database. Run 'python -m etl.cli run' first.")
            await conn.close()
            sys.exit(1)

        print(f"Found {len(datasets)} datasets to embed")
        print()

        # Setup embedding service (no API key needed)
        model_name = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        embedding_service = SentenceTransformerService(model_name=model_name)
        store = VectorStore(
            embedding_service=embedding_service,
            collection=conn.datasets,
            batch_size=32,
        )

        # Check existing
        existing = await store.get_indexed_ids()
        if existing:
            print(f"{len(existing)} datasets already embedded (will skip)")

        print("Generating embeddings...")
        print()

        result = await store.add_datasets(
            datasets,
            skip_existing=True,
            progress_callback=create_indexing_progress(),
        )

        print()
        print(result.summary())

        stats = await store.get_stats()
        print()
        print(f"Vector store: {stats['total_documents']} documents indexed")

        await conn.close()

    asyncio.run(run())


def cmd_status():
    """Show current status."""
    from etl.repository import MongoDBConnection, DatasetRepository
    from etl.embeddings import VectorStore, SentenceTransformerService

    async def run():
        print("DSH ETL Search AI - Status")
        print("=" * 50)
        print()

        try:
            conn = MongoDBConnection()
            await conn.connect()

            repo = DatasetRepository(conn.datasets)
            count = await repo.count()
            print("MongoDB")
            print(f"   URI: {conn.config.uri}")
            print(f"   Database: {conn.config.database_name}")
            print(f"   Datasets: {count}")
        except Exception as e:
            print("MongoDB")
            print(f"   Status: FAILED ({e})")

        print()

        # Vector store status
        print("Vector Store")
        try:
            embedding_service = SentenceTransformerService()
            store = VectorStore(
                embedding_service=embedding_service,
                collection=conn.datasets,
            )
            stats = await store.get_stats()
            print(f"   Indexed: {stats['total_documents']} documents")
            print(f"   Model: {stats['embedding_model']}")
            print(f"   Dimensions: {stats['embedding_dimensions']}")
        except Exception as e:
            print(f"   Status: not available ({e})")

        print()

        try:
            await conn.close()
        except Exception:
            pass

    asyncio.run(run())


def cmd_search(query: str):
    """Test search functionality."""
    from etl.repository import MongoDBConnection, DatasetRepository

    async def run():
        conn = MongoDBConnection()
        await conn.connect()

        repo = DatasetRepository(conn.datasets)

        print(f'Searching: "{query}"')
        print()

        # Try hybrid search
        try:
            from etl.embeddings import SentenceTransformerService, VectorStore
            from etl.search import HybridSearchService

            embedding_service = SentenceTransformerService()
            store = VectorStore(
                embedding_service=embedding_service,
                collection=conn.datasets,
            )

            service = HybridSearchService(
                vector_store=store,
                repository=repo,
            )

            response = await service.search(query, limit=5)

            print(f"Mode: Hybrid (semantic + keyword)")
            print(f"   Query type: {response.query_type.value}")
            print(f"   Semantic results: {response.total_semantic}")
            print(f"   Keyword results: {response.total_keyword}")
            print()

            if response.results:
                print("Results:")
                for i, r in enumerate(response.results, 1):
                    sources = []
                    if r.from_semantic:
                        sources.append(f"sem#{r.semantic_rank}")
                    if r.from_keyword:
                        sources.append(f"kw#{r.keyword_rank}")

                    print(f"  {i}. [{r.hybrid_score:.4f}] {r.title[:60]}...")
                    print(f"     Sources: {', '.join(sources)}")
                    print(f"     ID: {r.dataset_id}")
                    print()
            else:
                print("No results found.")

            await conn.close()
            return

        except Exception as e:
            print(f"Hybrid search failed ({type(e).__name__}), falling back to keyword search")
            print()

        # Keyword only fallback
        results = await repo.search(query, limit=5)

        print(f"Mode: Keyword only")
        print()

        if results:
            print("Results:")
            for i, d in enumerate(results, 1):
                print(f"  {i}. {d.title[:60]}...")
                print(f"     ID: {d.identifier}")
                print()
        else:
            print("No results found.")

        await conn.close()

    asyncio.run(run())


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="DSH ETL Search AI CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m etl.cli init                    Create MongoDB indexes
  python -m etl.cli run                     Run ETL pipeline
  python -m etl.cli embed                   Generate embeddings
  python -m etl.cli status                  Show status
  python -m etl.cli search "climate data"  Test search
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # init
    subparsers.add_parser("init", help="Initialize MongoDB indexes")

    # run
    run_parser = subparsers.add_parser("run", help="Run ETL pipeline")
    run_parser.add_argument(
        "--ids", "-i",
        default="data/metadata-file-identifiers.txt",
        help="File containing dataset IDs",
    )

    # embed
    subparsers.add_parser("embed", help="Generate embeddings")

    # status
    subparsers.add_parser("status", help="Show current status")

    # search
    search_parser = subparsers.add_parser("search", help="Test search")
    search_parser.add_argument("query", help="Search query")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init()
    elif args.command == "run":
        cmd_run(args.ids)
    elif args.command == "embed":
        cmd_embed()
    elif args.command == "status":
        cmd_status()
    elif args.command == "search":
        cmd_search(args.query)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
