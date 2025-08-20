"""
Database migration system for the web crawler.
Similar to Alembic but tailored for our specific needs.
"""

import asyncio
import hashlib
import time
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from pathlib import Path

import asyncpg

from crawler.utils.config import DatabaseConfig
from crawler.utils.exceptions import DatabaseError


@dataclass
class Migration:
    """Represents a database migration."""
    version: str
    name: str
    up_sql: str
    down_sql: str
    dependencies: List[str]
    checksum: str
    created_at: float
    
    @classmethod
    def create(cls, version: str, name: str, up_sql: str, down_sql: str = "", 
               dependencies: Optional[List[str]] = None) -> 'Migration':
        """Create a new migration."""
        content = f"{version}{name}{up_sql}{down_sql}"
        checksum = hashlib.md5(content.encode()).hexdigest()
        
        return cls(
            version=version,
            name=name,
            up_sql=up_sql,
            down_sql=down_sql,
            dependencies=dependencies or [],
            checksum=checksum,
            created_at=time.time()
        )


class MigrationManager:
    """
    Database migration manager with version control.
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self.migrations: Dict[str, Migration] = {}
        self._load_builtin_migrations()
    
    async def initialize(self) -> None:
        """Initialize database connection and migration table."""
        try:
            self.pool = await asyncpg.create_pool(
                self.config.url,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            
            # Create migration tracking table
            await self._create_migration_table()
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize migration manager: {e}")
    
    async def close(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
    
    def _load_builtin_migrations(self) -> None:
        """Load built-in migrations."""
        
        # Migration 001: Complete schema from database_schema.sql
        self.migrations["001"] = Migration.create(
            version="001",
            name="initial_complete_schema",
            up_sql="""
            -- Enable required extensions
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
            CREATE EXTENSION IF NOT EXISTS "pg_trgm" WITH SCHEMA public;
            CREATE EXTENSION IF NOT EXISTS "btree_gin" WITH SCHEMA public;
            
            -- Create function for URL queue trigger
            CREATE FUNCTION public.update_url_queue_updated_at() RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$;
            
            -- Create crawl_sessions table
            CREATE TABLE public.crawl_sessions (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                name character varying(255) NOT NULL,
                start_url text NOT NULL,
                max_depth integer DEFAULT 3,
                max_pages integer DEFAULT 1000,
                concurrent_workers integer DEFAULT 10,
                rate_limit_delay numeric(5,2) DEFAULT 1.0,
                status character varying(20) DEFAULT 'pending'::character varying,
                created_at timestamp with time zone DEFAULT now(),
                started_at timestamp with time zone,
                completed_at timestamp with time zone,
                total_pages_crawled integer DEFAULT 0,
                total_words_found bigint DEFAULT 0,
                total_unique_words integer DEFAULT 0,
                average_response_time numeric(8,3),
                error_count integer DEFAULT 0,
                configuration jsonb,
                description text,
                start_urls text[] DEFAULT '{}'::text[],
                pages_crawled integer DEFAULT 0,
                pages_failed integer DEFAULT 0,
                pages_skipped integer DEFAULT 0,
                total_words integer DEFAULT 0,
                total_bytes bigint DEFAULT 0,
                unique_domains integer DEFAULT 0,
                start_time timestamp with time zone,
                end_time timestamp with time zone,
                duration_seconds numeric(10,3),
                pages_per_second numeric(8,2),
                error_rate numeric(5,4)
            );
            
            -- Create pages table
            CREATE TABLE public.pages (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                session_id uuid NOT NULL,
                url text NOT NULL,
                url_hash character varying(64) NOT NULL,
                parent_url text,
                depth integer DEFAULT 0 NOT NULL,
                status_code integer,
                content_type character varying(100),
                content_length integer,
                title text,
                meta_description text,
                response_time numeric(8,3),
                crawled_at timestamp with time zone DEFAULT now(),
                error_message text,
                dns_lookup_time numeric(10,3),
                tcp_connect_time numeric(10,3),
                tls_handshake_time numeric(10,3),
                server_response_time numeric(10,3),
                content_download_time numeric(10,3),
                total_network_time numeric(10,3),
                html_parse_time numeric(10,3),
                text_extraction_time numeric(10,3),
                text_cleaning_time numeric(10,3),
                word_tokenization_time numeric(10,3),
                word_counting_time numeric(10,3),
                link_extraction_time numeric(10,3),
                total_processing_time numeric(10,3),
                db_insert_time numeric(10,3),
                db_query_time numeric(10,3),
                total_db_time numeric(10,3),
                queue_wait_time numeric(10,3),
                total_page_time numeric(10,3),
                raw_content_size integer,
                compressed_size integer,
                extracted_text_size integer,
                total_words integer DEFAULT 0,
                unique_words integer DEFAULT 0,
                average_word_length numeric(5,2),
                sentence_count integer,
                paragraph_count integer,
                total_html_tags integer,
                link_count integer DEFAULT 0,
                internal_link_count integer DEFAULT 0,
                external_link_count integer DEFAULT 0,
                image_count integer DEFAULT 0,
                form_count integer DEFAULT 0,
                script_tag_count integer DEFAULT 0,
                style_tag_count integer DEFAULT 0,
                text_to_html_ratio numeric(5,4),
                content_density numeric(8,4),
                readability_score numeric(5,2),
                final_url text,
                redirect_count integer DEFAULT 0,
                language character varying(10),
                charset character varying(50),
                remote_ip inet,
                connection_reused boolean DEFAULT false,
                protocol_version character varying(10),
                bandwidth_utilization numeric(12,2),
                last_modified timestamp with time zone,
                retry_count integer DEFAULT 0,
                additional_metrics jsonb,
                response_headers jsonb,
                raw_text_length integer DEFAULT 0,
                cleaned_text_length integer DEFAULT 0,
                unique_word_count integer DEFAULT 0,
                internal_links_count integer DEFAULT 0,
                external_links_count integer DEFAULT 0,
                total_links_count integer DEFAULT 0,
                quality_score numeric(5,4),
                processing_successful boolean DEFAULT true
            );
            
            -- Create word_frequencies table
            CREATE TABLE public.word_frequencies (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                page_id uuid NOT NULL,
                session_id uuid NOT NULL,
                word character varying(100) NOT NULL,
                frequency integer DEFAULT 1 NOT NULL,
                word_length integer NOT NULL,
                is_stopword boolean DEFAULT false,
                first_position integer,
                created_at timestamp with time zone DEFAULT now(),
                normalized_frequency numeric(8,6),
                tf_idf_score numeric(10,6),
                is_rare_word boolean DEFAULT false
            );
            
            -- Create links table
            CREATE TABLE public.links (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                session_id uuid NOT NULL,
                source_page_id uuid NOT NULL,
                target_url text NOT NULL,
                target_url_hash character varying(64) NOT NULL,
                target_page_id uuid,
                link_text text,
                link_type character varying(20) DEFAULT 'internal'::character varying,
                is_crawled boolean DEFAULT false,
                discovered_at timestamp with time zone DEFAULT now(),
                source_url text,
                link_title text,
                is_internal boolean DEFAULT true,
                discovery_depth integer DEFAULT 0,
                discovery_order integer DEFAULT 0,
                updated_at timestamp with time zone DEFAULT now()
            );
            
            -- Create error_events table
            CREATE TABLE public.error_events (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                session_id uuid,
                page_id uuid,
                occurred_at timestamp with time zone DEFAULT now(),
                error_type character varying(50) NOT NULL,
                error_category character varying(30) NOT NULL,
                error_severity character varying(20) DEFAULT 'medium'::character varying,
                error_message text,
                error_code character varying(20),
                stack_trace text,
                url text,
                depth integer,
                operation_name character varying(50),
                retry_count integer DEFAULT 0,
                max_retries integer DEFAULT 3,
                retry_delay_ms integer,
                resolved boolean DEFAULT false,
                resolution_time timestamp with time zone,
                resolution_method character varying(50),
                pages_affected integer DEFAULT 1,
                processing_time_lost_ms integer,
                user_agent character varying(255),
                remote_ip inet,
                http_status_code integer,
                error_metadata jsonb
            );
            
            -- Create url_queue table
            CREATE TABLE public.url_queue (
                id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
                session_id uuid NOT NULL,
                url text NOT NULL,
                url_hash character varying(64) NOT NULL,
                depth integer DEFAULT 0 NOT NULL,
                priority integer DEFAULT 0 NOT NULL,
                parent_url text,
                discovered_at timestamp with time zone DEFAULT now(),
                scheduled_at timestamp with time zone,
                last_attempt_at timestamp with time zone,
                attempts integer DEFAULT 0,
                status character varying(20) DEFAULT 'pending'::character varying,
                error_message text,
                metadata jsonb,
                created_at timestamp with time zone DEFAULT now(),
                updated_at timestamp with time zone DEFAULT now(),
                CONSTRAINT url_queue_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying, 'completed'::character varying, 'failed'::character varying])::text[])))
            );
            
            -- Add primary key constraints
            ALTER TABLE ONLY public.crawl_sessions ADD CONSTRAINT crawl_sessions_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.error_events ADD CONSTRAINT error_events_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.links ADD CONSTRAINT links_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.pages ADD CONSTRAINT pages_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.url_queue ADD CONSTRAINT url_queue_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.word_frequencies ADD CONSTRAINT word_frequencies_pkey PRIMARY KEY (id);
            
            -- Add unique constraints
            ALTER TABLE ONLY public.pages ADD CONSTRAINT pages_session_url_unique UNIQUE (session_id, url_hash);
            ALTER TABLE ONLY public.url_queue ADD CONSTRAINT url_queue_session_url_unique UNIQUE (session_id, url_hash);
            
            -- Create indexes for crawl_sessions
            CREATE INDEX idx_crawl_sessions_created_at ON public.crawl_sessions USING btree (created_at);
            CREATE INDEX idx_crawl_sessions_end_time ON public.crawl_sessions USING btree (end_time);
            CREATE INDEX idx_crawl_sessions_name ON public.crawl_sessions USING btree (name);
            CREATE INDEX idx_crawl_sessions_pages_crawled ON public.crawl_sessions USING btree (pages_crawled);
            CREATE INDEX idx_crawl_sessions_start_time ON public.crawl_sessions USING btree (start_time);
            CREATE INDEX idx_crawl_sessions_status ON public.crawl_sessions USING btree (status);
            
            -- Create indexes for pages
            CREATE INDEX idx_pages_additional_metrics ON public.pages USING gin (additional_metrics);
            CREATE INDEX idx_pages_content_size ON public.pages USING btree (raw_content_size);
            CREATE INDEX idx_pages_crawled_at ON public.pages USING btree (crawled_at);
            CREATE INDEX idx_pages_depth ON public.pages USING btree (depth);
            CREATE INDEX idx_pages_language ON public.pages USING btree (language);
            CREATE INDEX idx_pages_processing_successful ON public.pages USING btree (processing_successful);
            CREATE INDEX idx_pages_quality_score ON public.pages USING btree (quality_score);
            CREATE INDEX idx_pages_response_headers ON public.pages USING gin (response_headers);
            CREATE INDEX idx_pages_server_response_time ON public.pages USING btree (server_response_time);
            CREATE INDEX idx_pages_session_id ON public.pages USING btree (session_id);
            CREATE INDEX idx_pages_status_code ON public.pages USING btree (status_code);
            CREATE INDEX idx_pages_total_page_time ON public.pages USING btree (total_page_time);
            CREATE INDEX idx_pages_total_processing_time ON public.pages USING btree (total_processing_time);
            CREATE INDEX idx_pages_total_words ON public.pages USING btree (total_words);
            CREATE INDEX idx_pages_url_hash ON public.pages USING btree (url_hash);
            
            -- Create indexes for word_frequencies
            CREATE INDEX idx_word_frequencies_frequency ON public.word_frequencies USING btree (frequency DESC);
            CREATE INDEX idx_word_frequencies_is_rare ON public.word_frequencies USING btree (is_rare_word);
            CREATE INDEX idx_word_frequencies_normalized_freq ON public.word_frequencies USING btree (normalized_frequency DESC);
            CREATE INDEX idx_word_frequencies_page_id ON public.word_frequencies USING btree (page_id);
            CREATE INDEX idx_word_frequencies_session_freq ON public.word_frequencies USING btree (session_id, frequency DESC, word);
            CREATE INDEX idx_word_frequencies_session_id ON public.word_frequencies USING btree (session_id);
            CREATE INDEX idx_word_frequencies_session_word ON public.word_frequencies USING btree (session_id, word);
            CREATE INDEX idx_word_frequencies_tf_idf ON public.word_frequencies USING btree (tf_idf_score DESC);
            CREATE INDEX idx_word_frequencies_word ON public.word_frequencies USING btree (word);
            CREATE INDEX idx_word_frequencies_word_length ON public.word_frequencies USING btree (word_length);
            CREATE INDEX idx_word_frequencies_word_trgm ON public.word_frequencies USING gin (word public.gin_trgm_ops);
            
            -- Create indexes for links
            CREATE INDEX idx_links_discovered_at ON public.links USING btree (discovered_at);
            CREATE INDEX idx_links_discovery_depth ON public.links USING btree (discovery_depth);
            CREATE INDEX idx_links_is_crawled ON public.links USING btree (is_crawled);
            CREATE INDEX idx_links_is_internal ON public.links USING btree (is_internal);
            CREATE INDEX idx_links_link_type ON public.links USING btree (link_type);
            CREATE INDEX idx_links_session_id ON public.links USING btree (session_id);
            CREATE INDEX idx_links_source_page_id ON public.links USING btree (source_page_id);
            CREATE INDEX idx_links_source_url ON public.links USING btree (source_url);
            CREATE INDEX idx_links_target_page_id ON public.links USING btree (target_page_id);
            CREATE INDEX idx_links_target_url_hash ON public.links USING btree (target_url_hash);
            CREATE INDEX idx_links_updated_at ON public.links USING btree (updated_at);
            
            -- Create indexes for error_events
            CREATE INDEX idx_error_events_error_category ON public.error_events USING btree (error_category);
            CREATE INDEX idx_error_events_error_type ON public.error_events USING btree (error_type);
            CREATE INDEX idx_error_events_metadata ON public.error_events USING gin (error_metadata);
            CREATE INDEX idx_error_events_occurred_at ON public.error_events USING btree (occurred_at);
            CREATE INDEX idx_error_events_resolved ON public.error_events USING btree (resolved);
            CREATE INDEX idx_error_events_session_id ON public.error_events USING btree (session_id);
            CREATE INDEX idx_error_events_session_type_time ON public.error_events USING btree (session_id, error_type, occurred_at);
            
            -- Create indexes for url_queue
            CREATE INDEX idx_url_queue_discovered_at ON public.url_queue USING btree (discovered_at);
            CREATE INDEX idx_url_queue_metadata ON public.url_queue USING gin (metadata);
            CREATE INDEX idx_url_queue_priority ON public.url_queue USING btree (session_id, status, priority DESC, depth, discovered_at);
            CREATE INDEX idx_url_queue_session_id ON public.url_queue USING btree (session_id);
            CREATE INDEX idx_url_queue_session_status ON public.url_queue USING btree (session_id, status);
            CREATE INDEX idx_url_queue_status ON public.url_queue USING btree (status);
            CREATE INDEX idx_url_queue_updated_at ON public.url_queue USING btree (updated_at);
            CREATE INDEX idx_url_queue_url_hash ON public.url_queue USING btree (url_hash);
            
            -- Create trigger for url_queue
            CREATE TRIGGER url_queue_updated_at_trigger BEFORE UPDATE ON public.url_queue FOR EACH ROW EXECUTE FUNCTION public.update_url_queue_updated_at();
            
            -- Add foreign key constraints
            ALTER TABLE ONLY public.error_events ADD CONSTRAINT error_events_page_id_fkey FOREIGN KEY (page_id) REFERENCES public.pages(id) ON DELETE SET NULL;
            ALTER TABLE ONLY public.error_events ADD CONSTRAINT error_events_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.crawl_sessions(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.links ADD CONSTRAINT links_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.crawl_sessions(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.links ADD CONSTRAINT links_source_page_id_fkey FOREIGN KEY (source_page_id) REFERENCES public.pages(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.links ADD CONSTRAINT links_target_page_id_fkey FOREIGN KEY (target_page_id) REFERENCES public.pages(id) ON DELETE SET NULL;
            ALTER TABLE ONLY public.pages ADD CONSTRAINT pages_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.crawl_sessions(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.url_queue ADD CONSTRAINT url_queue_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.crawl_sessions(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.word_frequencies ADD CONSTRAINT word_frequencies_page_id_fkey FOREIGN KEY (page_id) REFERENCES public.pages(id) ON DELETE CASCADE;
            ALTER TABLE ONLY public.word_frequencies ADD CONSTRAINT word_frequencies_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.crawl_sessions(id) ON DELETE CASCADE;
            """,
            down_sql="""
            -- Drop all tables in reverse dependency order
            DROP TABLE IF EXISTS error_events CASCADE;
            DROP TABLE IF EXISTS url_queue CASCADE;
            DROP TABLE IF EXISTS links CASCADE;
            DROP TABLE IF EXISTS word_frequencies CASCADE;
            DROP TABLE IF EXISTS pages CASCADE;
            DROP TABLE IF EXISTS crawl_sessions CASCADE;
            
            -- Drop function
            DROP FUNCTION IF EXISTS public.update_url_queue_updated_at();
            
            -- Drop extensions
            DROP EXTENSION IF EXISTS "btree_gin";
            DROP EXTENSION IF EXISTS "pg_trgm";
            DROP EXTENSION IF EXISTS "uuid-ossp";
            """
        )
    
    async def _create_migration_table(self) -> None:
        """Create the migration tracking table."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    checksum VARCHAR(32) NOT NULL,
                    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    execution_time_ms INTEGER,
                    success BOOLEAN DEFAULT TRUE,
                    error_message TEXT
                )
            """)
    
    async def get_current_version(self) -> Optional[str]:
        """Get the current schema version."""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT version FROM schema_migrations 
                WHERE success = TRUE 
                ORDER BY applied_at DESC 
                LIMIT 1
            """)
            return result
    
    async def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions."""
        async with self.pool.acquire() as conn:
            results = await conn.fetch("""
                SELECT version FROM schema_migrations 
                WHERE success = TRUE 
                ORDER BY applied_at
            """)
            return [row['version'] for row in results]
    
    async def get_pending_migrations(self) -> List[Migration]:
        """Get list of pending migrations."""
        applied = await self.get_applied_migrations()
        pending = []
        
        for version in sorted(self.migrations.keys()):
            if version not in applied:
                migration = self.migrations[version]
                # Check if dependencies are satisfied
                if all(dep in applied for dep in migration.dependencies):
                    pending.append(migration)
        
        return pending
    
    async def apply_migration(self, migration: Migration) -> bool:
        """Apply a single migration."""
        start_time = time.time()
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Execute the migration SQL
                    await conn.execute(migration.up_sql)
                    
                    # Record successful migration
                    execution_time = int((time.time() - start_time) * 1000)
                    await conn.execute("""
                        INSERT INTO schema_migrations 
                        (version, name, checksum, execution_time_ms, success)
                        VALUES ($1, $2, $3, $4, $5)
                    """, migration.version, migration.name, migration.checksum, 
                        execution_time, True)
                    
                    print(f"✓ Applied migration {migration.version}: {migration.name}")
                    return True
                    
                except Exception as e:
                    # Record failed migration
                    execution_time = int((time.time() - start_time) * 1000)
                    await conn.execute("""
                        INSERT INTO schema_migrations 
                        (version, name, checksum, execution_time_ms, success, error_message)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, migration.version, migration.name, migration.checksum, 
                        execution_time, False, str(e))
                    
                    print(f"✗ Failed to apply migration {migration.version}: {e}")
                    return False
    
    async def rollback_migration(self, migration: Migration) -> bool:
        """Rollback a single migration."""
        if not migration.down_sql.strip():
            print(f"✗ No rollback SQL for migration {migration.version}")
            return False
        
        start_time = time.time()
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Execute the rollback SQL
                    await conn.execute(migration.down_sql)
                    
                    # Remove migration record
                    await conn.execute("""
                        DELETE FROM schema_migrations WHERE version = $1
                    """, migration.version)
                    
                    print(f"✓ Rolled back migration {migration.version}: {migration.name}")
                    return True
                    
                except Exception as e:
                    print(f"✗ Failed to rollback migration {migration.version}: {e}")
                    return False
    
    async def migrate_to_latest(self) -> bool:
        """Apply all pending migrations."""
        pending = await self.get_pending_migrations()
        
        if not pending:
            print("✓ Database is up to date")
            return True
        
        print(f"Applying {len(pending)} pending migrations...")
        
        for migration in pending:
            success = await self.apply_migration(migration)
            if not success:
                print(f"✗ Migration failed, stopping at {migration.version}")
                return False
        
        print("✓ All migrations applied successfully")
        return True
    
    async def migrate_to_version(self, target_version: str) -> bool:
        """Migrate to a specific version."""
        current_version = await self.get_current_version()
        applied = await self.get_applied_migrations()
        
        if target_version in applied:
            print(f"✓ Already at version {target_version}")
            return True
        
        if target_version not in self.migrations:
            print(f"✗ Unknown migration version: {target_version}")
            return False
        
        # Find path to target version
        pending = []
        for version in sorted(self.migrations.keys()):
            if version not in applied and version <= target_version:
                migration = self.migrations[version]
                if all(dep in applied or dep in [m.version for m in pending] for dep in migration.dependencies):
                    pending.append(migration)
        
        print(f"Applying {len(pending)} migrations to reach version {target_version}...")
        
        for migration in pending:
            success = await self.apply_migration(migration)
            if not success:
                return False
            applied.append(migration.version)
        
        return True
    
    async def rollback_to_version(self, target_version: str) -> bool:
        """Rollback to a specific version."""
        applied = await self.get_applied_migrations()
        
        if target_version not in applied:
            print(f"✗ Version {target_version} was never applied")
            return False
        
        # Find migrations to rollback (in reverse order)
        to_rollback = []
        for version in reversed(applied):
            if version > target_version:
                to_rollback.append(self.migrations[version])
        
        if not to_rollback:
            print(f"✓ Already at version {target_version}")
            return True
        
        print(f"Rolling back {len(to_rollback)} migrations to version {target_version}...")
        
        for migration in to_rollback:
            success = await self.rollback_migration(migration)
            if not success:
                return False
        
        return True
    
    async def recreate_schema(self) -> bool:
        """Drop all tables and recreate from scratch."""
        print("⚠️  Recreating schema - this will destroy all data!")
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Drop all tables in reverse dependency order (including old ones)
                    drop_tables = [
                        "error_events",
                        "system_metrics_timeseries",
                        "session_metrics_timeseries",
                        "url_queue",
                        "links",
                        "word_frequencies",
                        "pages",
                        "crawl_sessions",
                        "metrics",
                        "errors",
                        "schema_migrations"
                    ]
                    
                    for table in drop_tables:
                        await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    
                    # Drop function
                    await conn.execute('DROP FUNCTION IF EXISTS public.update_url_queue_updated_at() CASCADE')
                    
                    # Drop extensions with CASCADE to handle dependencies
                    await conn.execute('DROP EXTENSION IF EXISTS "btree_gin" CASCADE')
                    await conn.execute('DROP EXTENSION IF EXISTS "pg_trgm" CASCADE')
                    await conn.execute('DROP EXTENSION IF EXISTS "uuid-ossp" CASCADE')
                    
                    print("✓ Dropped all existing tables and extensions")
                    
                except Exception as e:
                    print(f"Warning: Error dropping tables: {e}")
        
        # Recreate migration table
        await self._create_migration_table()
        
        # Apply all migrations
        return await self.migrate_to_latest()
    
    async def get_migration_status(self) -> Dict[str, Any]:
        """Get comprehensive migration status."""
        current_version = await self.get_current_version()
        applied = await self.get_applied_migrations()
        pending = await self.get_pending_migrations()
        
        return {
            'current_version': current_version,
            'applied_migrations': applied,
            'pending_migrations': [m.version for m in pending],
            'total_migrations': len(self.migrations),
            'applied_count': len(applied),
            'pending_count': len(pending),
            'is_up_to_date': len(pending) == 0
        }