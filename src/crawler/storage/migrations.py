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
        
        # Migration 001: Initial schema
        self.migrations["001"] = Migration.create(
            version="001",
            name="initial_schema",
            up_sql="""
            -- Enable required extensions
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE EXTENSION IF NOT EXISTS "pg_trgm";
            CREATE EXTENSION IF NOT EXISTS "btree_gin";
            
            -- Create crawl_sessions table
            CREATE TABLE crawl_sessions (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(255) NOT NULL,
                start_url TEXT NOT NULL,
                max_depth INTEGER DEFAULT 3,
                max_pages INTEGER DEFAULT 1000,
                concurrent_workers INTEGER DEFAULT 10,
                rate_limit_delay DECIMAL(5,2) DEFAULT 1.0,
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                total_pages_crawled INTEGER DEFAULT 0,
                total_words_found BIGINT DEFAULT 0,
                total_unique_words INTEGER DEFAULT 0,
                average_response_time DECIMAL(8,3),
                error_count INTEGER DEFAULT 0,
                configuration JSONB
            );
            
            -- Create basic indexes for crawl_sessions
            CREATE INDEX idx_crawl_sessions_status ON crawl_sessions(status);
            CREATE INDEX idx_crawl_sessions_created_at ON crawl_sessions(created_at);
            CREATE INDEX idx_crawl_sessions_name ON crawl_sessions(name);
            """,
            down_sql="""
            DROP TABLE IF EXISTS crawl_sessions CASCADE;
            DROP EXTENSION IF EXISTS "btree_gin";
            DROP EXTENSION IF EXISTS "pg_trgm";
            DROP EXTENSION IF EXISTS "uuid-ossp";
            """
        )
        
        # Migration 002: Pages table with basic metrics
        self.migrations["002"] = Migration.create(
            version="002",
            name="pages_table_basic",
            up_sql="""
            CREATE TABLE pages (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                url_hash VARCHAR(64) NOT NULL,
                parent_url TEXT,
                depth INTEGER NOT NULL DEFAULT 0,
                
                -- HTTP Response Information
                status_code INTEGER,
                content_type VARCHAR(100),
                content_length INTEGER,
                title TEXT,
                meta_description TEXT,
                
                -- Basic timing metrics
                response_time DECIMAL(8,3),
                crawled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                -- Content metrics
                word_count INTEGER DEFAULT 0,
                
                -- Error information
                error_message TEXT,
                
                CONSTRAINT pages_session_url_unique UNIQUE (session_id, url_hash)
            );
            
            -- Basic indexes
            CREATE INDEX idx_pages_session_id ON pages(session_id);
            CREATE INDEX idx_pages_url_hash ON pages(url_hash);
            CREATE INDEX idx_pages_depth ON pages(depth);
            CREATE INDEX idx_pages_status_code ON pages(status_code);
            CREATE INDEX idx_pages_crawled_at ON pages(crawled_at);
            """,
            down_sql="""
            DROP TABLE IF EXISTS pages CASCADE;
            """,
            dependencies=["001"]
        )
        
        # Migration 003: Enhanced timing metrics
        self.migrations["003"] = Migration.create(
            version="003",
            name="enhanced_timing_metrics",
            up_sql="""
            -- Add detailed timing columns to pages table
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS dns_lookup_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS tcp_connect_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS tls_handshake_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS server_response_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS content_download_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_network_time DECIMAL(10,3);
            
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS html_parse_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS text_extraction_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS text_cleaning_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS word_tokenization_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS word_counting_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS link_extraction_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_processing_time DECIMAL(10,3);
            
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS db_insert_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS db_query_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_db_time DECIMAL(10,3);
            
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS queue_wait_time DECIMAL(10,3);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_page_time DECIMAL(10,3);
            
            -- Add indexes for timing metrics
            CREATE INDEX IF NOT EXISTS idx_pages_server_response_time ON pages(server_response_time);
            CREATE INDEX IF NOT EXISTS idx_pages_total_processing_time ON pages(total_processing_time);
            CREATE INDEX IF NOT EXISTS idx_pages_total_page_time ON pages(total_page_time);
            """,
            down_sql="""
            -- Remove timing columns
            ALTER TABLE pages DROP COLUMN IF EXISTS dns_lookup_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS tcp_connect_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS tls_handshake_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS server_response_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS content_download_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS total_network_time;
            
            ALTER TABLE pages DROP COLUMN IF EXISTS html_parse_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS text_extraction_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS text_cleaning_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS word_tokenization_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS word_counting_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS link_extraction_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS total_processing_time;
            
            ALTER TABLE pages DROP COLUMN IF EXISTS db_insert_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS db_query_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS total_db_time;
            
            ALTER TABLE pages DROP COLUMN IF EXISTS queue_wait_time;
            ALTER TABLE pages DROP COLUMN IF EXISTS total_page_time;
            
            -- Drop indexes
            DROP INDEX IF EXISTS idx_pages_server_response_time;
            DROP INDEX IF EXISTS idx_pages_total_processing_time;
            DROP INDEX IF EXISTS idx_pages_total_page_time;
            """,
            dependencies=["002"]
        )
        
        # Migration 004: Content quality and structure metrics
        self.migrations["004"] = Migration.create(
            version="004",
            name="content_quality_metrics",
            up_sql="""
            -- Add content analysis columns
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS raw_content_size INTEGER;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS compressed_size INTEGER;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS extracted_text_size INTEGER;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_words INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS unique_words INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS average_word_length DECIMAL(5,2);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS sentence_count INTEGER;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS paragraph_count INTEGER;
            
            -- HTML structure metrics
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_html_tags INTEGER;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS link_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS internal_link_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS external_link_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS image_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS form_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS script_tag_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS style_tag_count INTEGER DEFAULT 0;
            
            -- Quality metrics
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS text_to_html_ratio DECIMAL(5,4);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS content_density DECIMAL(8,4);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS readability_score DECIMAL(5,2);
            
            -- Network and metadata
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS final_url TEXT;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS redirect_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS language VARCHAR(10);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS charset VARCHAR(50);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS remote_ip INET;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS connection_reused BOOLEAN DEFAULT FALSE;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS protocol_version VARCHAR(10);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS bandwidth_utilization DECIMAL(12,2);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_modified TIMESTAMP WITH TIME ZONE;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS additional_metrics JSONB;
            
            -- Update word_count column name for consistency
            ALTER TABLE pages RENAME COLUMN word_count TO old_word_count;
            UPDATE pages SET total_words = old_word_count WHERE old_word_count IS NOT NULL;
            ALTER TABLE pages DROP COLUMN old_word_count;
            
            -- Add indexes for new columns
            CREATE INDEX IF NOT EXISTS idx_pages_total_words ON pages(total_words);
            CREATE INDEX IF NOT EXISTS idx_pages_content_size ON pages(raw_content_size);
            CREATE INDEX IF NOT EXISTS idx_pages_language ON pages(language);
            CREATE INDEX IF NOT EXISTS idx_pages_additional_metrics ON pages USING gin(additional_metrics);
            """,
            down_sql="""
            -- Remove content analysis columns
            ALTER TABLE pages DROP COLUMN IF EXISTS raw_content_size;
            ALTER TABLE pages DROP COLUMN IF EXISTS compressed_size;
            ALTER TABLE pages DROP COLUMN IF EXISTS extracted_text_size;
            ALTER TABLE pages DROP COLUMN IF EXISTS unique_words;
            ALTER TABLE pages DROP COLUMN IF EXISTS average_word_length;
            ALTER TABLE pages DROP COLUMN IF EXISTS sentence_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS paragraph_count;
            
            -- Remove HTML structure metrics
            ALTER TABLE pages DROP COLUMN IF EXISTS total_html_tags;
            ALTER TABLE pages DROP COLUMN IF EXISTS link_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS internal_link_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS external_link_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS image_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS form_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS script_tag_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS style_tag_count;
            
            -- Remove quality metrics
            ALTER TABLE pages DROP COLUMN IF EXISTS text_to_html_ratio;
            ALTER TABLE pages DROP COLUMN IF EXISTS content_density;
            ALTER TABLE pages DROP COLUMN IF EXISTS readability_score;
            
            -- Remove network and metadata
            ALTER TABLE pages DROP COLUMN IF EXISTS final_url;
            ALTER TABLE pages DROP COLUMN IF EXISTS redirect_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS language;
            ALTER TABLE pages DROP COLUMN IF EXISTS charset;
            ALTER TABLE pages DROP COLUMN IF EXISTS remote_ip;
            ALTER TABLE pages DROP COLUMN IF EXISTS connection_reused;
            ALTER TABLE pages DROP COLUMN IF EXISTS protocol_version;
            ALTER TABLE pages DROP COLUMN IF EXISTS bandwidth_utilization;
            ALTER TABLE pages DROP COLUMN IF EXISTS last_modified;
            ALTER TABLE pages DROP COLUMN IF EXISTS retry_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS additional_metrics;
            
            -- Restore old word_count column
            ALTER TABLE pages ADD COLUMN word_count INTEGER DEFAULT 0;
            UPDATE pages SET word_count = total_words WHERE total_words IS NOT NULL;
            ALTER TABLE pages DROP COLUMN total_words;
            """,
            dependencies=["003"]
        )
        
        # Migration 005: Word frequencies and links tables
        self.migrations["005"] = Migration.create(
            version="005",
            name="word_frequencies_and_links",
            up_sql="""
            -- Create word_frequencies table
            CREATE TABLE word_frequencies (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                page_id UUID NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                word VARCHAR(100) NOT NULL,
                frequency INTEGER NOT NULL DEFAULT 1,
                word_length INTEGER NOT NULL,
                is_stopword BOOLEAN DEFAULT FALSE,
                first_position INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Create links table
            CREATE TABLE links (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                source_page_id UUID NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                target_url TEXT NOT NULL,
                target_url_hash VARCHAR(64) NOT NULL,
                target_page_id UUID REFERENCES pages(id) ON DELETE SET NULL,
                link_text TEXT,
                link_type VARCHAR(20) DEFAULT 'internal',
                is_crawled BOOLEAN DEFAULT FALSE,
                discovered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Indexes for word_frequencies
            CREATE INDEX idx_word_frequencies_page_id ON word_frequencies(page_id);
            CREATE INDEX idx_word_frequencies_session_id ON word_frequencies(session_id);
            CREATE INDEX idx_word_frequencies_word ON word_frequencies(word);
            CREATE INDEX idx_word_frequencies_frequency ON word_frequencies(frequency DESC);
            CREATE INDEX idx_word_frequencies_word_length ON word_frequencies(word_length);
            CREATE INDEX idx_word_frequencies_session_word ON word_frequencies(session_id, word);
            CREATE INDEX idx_word_frequencies_session_freq ON word_frequencies(session_id, frequency DESC, word);
            CREATE INDEX idx_word_frequencies_word_trgm ON word_frequencies USING gin(word gin_trgm_ops);
            
            -- Indexes for links
            CREATE INDEX idx_links_session_id ON links(session_id);
            CREATE INDEX idx_links_source_page_id ON links(source_page_id);
            CREATE INDEX idx_links_target_url_hash ON links(target_url_hash);
            CREATE INDEX idx_links_target_page_id ON links(target_page_id);
            CREATE INDEX idx_links_link_type ON links(link_type);
            CREATE INDEX idx_links_is_crawled ON links(is_crawled);
            CREATE INDEX idx_links_discovered_at ON links(discovered_at);
            """,
            down_sql="""
            DROP TABLE IF EXISTS links CASCADE;
            DROP TABLE IF EXISTS word_frequencies CASCADE;
            """,
            dependencies=["004"]
        )
        
        # Migration 006: Time-series metrics tables
        self.migrations["006"] = Migration.create(
            version="006",
            name="timeseries_metrics",
            up_sql="""
            -- Session metrics time-series
            CREATE TABLE session_metrics_timeseries (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                -- Performance metrics
                pages_per_second DECIMAL(8,2),
                words_per_second DECIMAL(10,2),
                bytes_per_second DECIMAL(12,2),
                
                -- Timing aggregates
                avg_response_time DECIMAL(8,3),
                avg_processing_time DECIMAL(8,3),
                avg_total_time DECIMAL(8,3),
                p50_response_time DECIMAL(8,3),
                p95_response_time DECIMAL(8,3),
                p99_response_time DECIMAL(8,3),
                
                -- Queue metrics
                urls_in_queue INTEGER,
                urls_processed INTEGER,
                urls_failed INTEGER,
                queue_processing_rate DECIMAL(8,2),
                
                -- Error metrics
                total_error_rate DECIMAL(5,2),
                network_error_rate DECIMAL(5,2),
                processing_error_rate DECIMAL(5,2),
                database_error_rate DECIMAL(5,2),
                
                -- Resource utilization
                cpu_usage_percent DECIMAL(5,2),
                memory_usage_mb INTEGER,
                memory_growth_rate DECIMAL(10,2),
                active_connections INTEGER,
                
                -- Efficiency metrics
                cache_hit_ratio DECIMAL(5,4),
                compression_ratio DECIMAL(5,4),
                connection_reuse_ratio DECIMAL(5,4)
            );
            
            -- System metrics time-series
            CREATE TABLE system_metrics_timeseries (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                -- CPU metrics
                cpu_usage_percent DECIMAL(5,2),
                cpu_usage_per_core DECIMAL(5,2)[],
                cpu_load_1min DECIMAL(5,2),
                cpu_load_5min DECIMAL(5,2),
                cpu_load_15min DECIMAL(5,2),
                context_switches_per_sec INTEGER,
                
                -- Memory metrics
                total_memory_mb INTEGER,
                used_memory_mb INTEGER,
                available_memory_mb INTEGER,
                memory_usage_percent DECIMAL(5,2),
                swap_usage_mb INTEGER,
                process_memory_rss_mb INTEGER,
                process_memory_vms_mb INTEGER,
                
                -- Network metrics
                network_bytes_sent BIGINT,
                network_bytes_received BIGINT,
                network_packets_sent BIGINT,
                network_packets_received BIGINT,
                active_network_connections INTEGER,
                
                -- Disk I/O metrics
                disk_read_bytes BIGINT,
                disk_write_bytes BIGINT,
                disk_read_ops INTEGER,
                disk_write_ops INTEGER,
                disk_usage_percent DECIMAL(5,2),
                
                system_load_info JSONB
            );
            
            -- Error events table
            CREATE TABLE error_events (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                page_id UUID REFERENCES pages(id) ON DELETE SET NULL,
                occurred_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                error_type VARCHAR(50) NOT NULL,
                error_category VARCHAR(30) NOT NULL,
                error_severity VARCHAR(20) DEFAULT 'medium',
                error_message TEXT,
                error_code VARCHAR(20),
                stack_trace TEXT,
                
                url TEXT,
                depth INTEGER,
                operation_name VARCHAR(50),
                
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                retry_delay_ms INTEGER,
                
                resolved BOOLEAN DEFAULT FALSE,
                resolution_time TIMESTAMP WITH TIME ZONE,
                resolution_method VARCHAR(50),
                
                pages_affected INTEGER DEFAULT 1,
                processing_time_lost_ms INTEGER,
                
                user_agent VARCHAR(255),
                remote_ip INET,
                http_status_code INTEGER,
                
                error_metadata JSONB
            );
            
            -- Indexes for time-series tables
            CREATE INDEX idx_session_metrics_session_time ON session_metrics_timeseries(session_id, recorded_at);
            CREATE INDEX idx_session_metrics_recorded_at ON session_metrics_timeseries(recorded_at);
            
            CREATE INDEX idx_system_metrics_recorded_at ON system_metrics_timeseries(recorded_at);
            
            CREATE INDEX idx_error_events_session_id ON error_events(session_id);
            CREATE INDEX idx_error_events_occurred_at ON error_events(occurred_at);
            CREATE INDEX idx_error_events_error_type ON error_events(error_type);
            CREATE INDEX idx_error_events_error_category ON error_events(error_category);
            CREATE INDEX idx_error_events_resolved ON error_events(resolved);
            CREATE INDEX idx_error_events_session_type_time ON error_events(session_id, error_type, occurred_at);
            CREATE INDEX idx_error_events_metadata ON error_events USING gin(error_metadata);
            """,
            down_sql="""
            DROP TABLE IF EXISTS error_events CASCADE;
            DROP TABLE IF EXISTS system_metrics_timeseries CASCADE;
            DROP TABLE IF EXISTS session_metrics_timeseries CASCADE;
            """,
            dependencies=["005"]
        )
        
        # Migration 007: URL queue persistence table
        self.migrations["007"] = Migration.create(
            version="007",
            name="url_queue_persistence",
            up_sql="""
            -- Create URL queue table for persistent queue management
            CREATE TABLE url_queue (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                url_hash VARCHAR(64) NOT NULL,
                depth INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 0,
                parent_url TEXT,
                
                -- Timing information
                discovered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                scheduled_at TIMESTAMP WITH TIME ZONE,
                last_attempt_at TIMESTAMP WITH TIME ZONE,
                
                -- Queue management
                attempts INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                error_message TEXT,
                
                -- Metadata
                metadata JSONB,
                
                -- Tracking
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                -- Constraints
                CONSTRAINT url_queue_session_url_unique UNIQUE (session_id, url_hash),
                CONSTRAINT url_queue_status_check CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
            );
            
            -- Indexes for efficient queue operations
            CREATE INDEX idx_url_queue_session_id ON url_queue(session_id);
            CREATE INDEX idx_url_queue_status ON url_queue(status);
            CREATE INDEX idx_url_queue_priority ON url_queue(session_id, status, priority DESC, depth ASC, discovered_at ASC);
            CREATE INDEX idx_url_queue_url_hash ON url_queue(url_hash);
            CREATE INDEX idx_url_queue_discovered_at ON url_queue(discovered_at);
            CREATE INDEX idx_url_queue_updated_at ON url_queue(updated_at);
            CREATE INDEX idx_url_queue_session_status ON url_queue(session_id, status);
            CREATE INDEX idx_url_queue_metadata ON url_queue USING gin(metadata);
            
            -- Function to automatically update updated_at timestamp
            CREATE OR REPLACE FUNCTION update_url_queue_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Trigger to automatically update updated_at
            CREATE TRIGGER url_queue_updated_at_trigger
                BEFORE UPDATE ON url_queue
                FOR EACH ROW
                EXECUTE FUNCTION update_url_queue_updated_at();
            """,
            down_sql="""
            DROP TRIGGER IF EXISTS url_queue_updated_at_trigger ON url_queue;
            DROP FUNCTION IF EXISTS update_url_queue_updated_at();
            DROP TABLE IF EXISTS url_queue CASCADE;
            """,
            dependencies=["006"]
        )
        
        # Migration 008: Sync with models.py - Update crawl_sessions table
        self.migrations["008"] = Migration.create(
            version="008",
            name="sync_crawl_sessions_with_models",
            up_sql="""
            -- Add missing fields to crawl_sessions table to match CrawlSessionModel
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS description TEXT;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS start_urls TEXT[] DEFAULT '{}';
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS pages_crawled INTEGER DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS pages_failed INTEGER DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS pages_skipped INTEGER DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS total_words INTEGER DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS total_bytes BIGINT DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS unique_domains INTEGER DEFAULT 0;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS start_time TIMESTAMP WITH TIME ZONE;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS end_time TIMESTAMP WITH TIME ZONE;
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS duration_seconds DECIMAL(10,3);
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS pages_per_second DECIMAL(8,2);
            ALTER TABLE crawl_sessions ADD COLUMN IF NOT EXISTS error_rate DECIMAL(5,4);
            
            -- Migrate existing data
            UPDATE crawl_sessions SET
                start_urls = ARRAY[start_url] WHERE start_url IS NOT NULL AND start_urls = '{}';
            UPDATE crawl_sessions SET
                pages_crawled = total_pages_crawled WHERE total_pages_crawled IS NOT NULL;
            UPDATE crawl_sessions SET
                total_words = total_words_found WHERE total_words_found IS NOT NULL;
            UPDATE crawl_sessions SET
                start_time = started_at WHERE started_at IS NOT NULL;
            UPDATE crawl_sessions SET
                end_time = completed_at WHERE completed_at IS NOT NULL;
            
            -- Add indexes for new fields
            CREATE INDEX IF NOT EXISTS idx_crawl_sessions_pages_crawled ON crawl_sessions(pages_crawled);
            CREATE INDEX IF NOT EXISTS idx_crawl_sessions_start_time ON crawl_sessions(start_time);
            CREATE INDEX IF NOT EXISTS idx_crawl_sessions_end_time ON crawl_sessions(end_time);
            """,
            down_sql="""
            -- Remove added fields
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS description;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS start_urls;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS pages_crawled;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS pages_failed;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS pages_skipped;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS total_words;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS total_bytes;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS unique_domains;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS start_time;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS end_time;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS duration_seconds;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS pages_per_second;
            ALTER TABLE crawl_sessions DROP COLUMN IF EXISTS error_rate;
            
            -- Drop indexes
            DROP INDEX IF EXISTS idx_crawl_sessions_pages_crawled;
            DROP INDEX IF EXISTS idx_crawl_sessions_start_time;
            DROP INDEX IF EXISTS idx_crawl_sessions_end_time;
            """,
            dependencies=["007"]
        )
        
        # Migration 009: Sync pages table with PageModel
        self.migrations["009"] = Migration.create(
            version="009",
            name="sync_pages_with_models",
            up_sql="""
            -- Add missing fields to pages table to match PageModel
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS response_headers JSONB;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS raw_text_length INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS cleaned_text_length INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS unique_word_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS internal_links_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS external_links_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS total_links_count INTEGER DEFAULT 0;
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS quality_score DECIMAL(5,4);
            ALTER TABLE pages ADD COLUMN IF NOT EXISTS processing_successful BOOLEAN DEFAULT TRUE;
            
            -- Update existing data
            UPDATE pages SET
                internal_links_count = internal_link_count WHERE internal_link_count IS NOT NULL;
            UPDATE pages SET
                external_links_count = external_link_count WHERE external_link_count IS NOT NULL;
            UPDATE pages SET
                total_links_count = link_count WHERE link_count IS NOT NULL;
            UPDATE pages SET
                raw_text_length = raw_content_size WHERE raw_content_size IS NOT NULL;
            UPDATE pages SET
                cleaned_text_length = extracted_text_size WHERE extracted_text_size IS NOT NULL;
            UPDATE pages SET
                unique_word_count = unique_words WHERE unique_words IS NOT NULL;
            
            -- Add indexes for new fields
            CREATE INDEX IF NOT EXISTS idx_pages_response_headers ON pages USING gin(response_headers);
            CREATE INDEX IF NOT EXISTS idx_pages_quality_score ON pages(quality_score);
            CREATE INDEX IF NOT EXISTS idx_pages_processing_successful ON pages(processing_successful);
            """,
            down_sql="""
            -- Remove added fields
            ALTER TABLE pages DROP COLUMN IF EXISTS response_headers;
            ALTER TABLE pages DROP COLUMN IF EXISTS raw_text_length;
            ALTER TABLE pages DROP COLUMN IF EXISTS cleaned_text_length;
            ALTER TABLE pages DROP COLUMN IF EXISTS unique_word_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS internal_links_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS external_links_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS total_links_count;
            ALTER TABLE pages DROP COLUMN IF EXISTS quality_score;
            ALTER TABLE pages DROP COLUMN IF EXISTS processing_successful;
            
            -- Drop indexes
            DROP INDEX IF EXISTS idx_pages_response_headers;
            DROP INDEX IF EXISTS idx_pages_quality_score;
            DROP INDEX IF EXISTS idx_pages_processing_successful;
            """,
            dependencies=["008"]
        )
        
        # Migration 010: Sync word_frequencies table with WordFrequencyModel
        self.migrations["010"] = Migration.create(
            version="010",
            name="sync_word_frequencies_with_models",
            up_sql="""
            -- Add missing fields to word_frequencies table to match WordFrequencyModel
            ALTER TABLE word_frequencies ADD COLUMN IF NOT EXISTS normalized_frequency DECIMAL(8,6);
            ALTER TABLE word_frequencies ADD COLUMN IF NOT EXISTS tf_idf_score DECIMAL(10,6);
            ALTER TABLE word_frequencies ADD COLUMN IF NOT EXISTS is_rare_word BOOLEAN DEFAULT FALSE;
            
            -- Add indexes for new fields
            CREATE INDEX IF NOT EXISTS idx_word_frequencies_normalized_freq ON word_frequencies(normalized_frequency DESC);
            CREATE INDEX IF NOT EXISTS idx_word_frequencies_tf_idf ON word_frequencies(tf_idf_score DESC);
            CREATE INDEX IF NOT EXISTS idx_word_frequencies_is_rare ON word_frequencies(is_rare_word);
            """,
            down_sql="""
            -- Remove added fields
            ALTER TABLE word_frequencies DROP COLUMN IF EXISTS normalized_frequency;
            ALTER TABLE word_frequencies DROP COLUMN IF EXISTS tf_idf_score;
            ALTER TABLE word_frequencies DROP COLUMN IF EXISTS is_rare_word;
            
            -- Drop indexes
            DROP INDEX IF EXISTS idx_word_frequencies_normalized_freq;
            DROP INDEX IF EXISTS idx_word_frequencies_tf_idf;
            DROP INDEX IF EXISTS idx_word_frequencies_is_rare;
            """,
            dependencies=["009"]
        )
        
        # Migration 011: Create metrics table for MetricsModel
        self.migrations["011"] = Migration.create(
            version="011",
            name="create_metrics_table",
            up_sql="""
            -- Create metrics table to match MetricsModel
            CREATE TABLE IF NOT EXISTS metrics (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                page_id UUID REFERENCES pages(id) ON DELETE CASCADE,
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                
                -- Timing metrics (milliseconds)
                dns_lookup_time DECIMAL(10,3),
                tcp_connect_time DECIMAL(10,3),
                tls_handshake_time DECIMAL(10,3),
                request_send_time DECIMAL(10,3),
                server_response_time DECIMAL(10,3),
                content_download_time DECIMAL(10,3),
                total_network_time DECIMAL(10,3),
                
                -- Processing timing (milliseconds)
                html_parse_time DECIMAL(10,3),
                text_extraction_time DECIMAL(10,3),
                text_cleaning_time DECIMAL(10,3),
                word_tokenization_time DECIMAL(10,3),
                word_counting_time DECIMAL(10,3),
                link_extraction_time DECIMAL(10,3),
                total_processing_time DECIMAL(10,3),
                
                -- Database timing (milliseconds)
                db_insert_time DECIMAL(10,3),
                db_query_time DECIMAL(10,3),
                total_db_time DECIMAL(10,3),
                
                -- Overall timing
                queue_wait_time DECIMAL(10,3),
                total_time DECIMAL(10,3),
                
                -- Resource metrics
                memory_usage_mb DECIMAL(10,2),
                cpu_usage_percent DECIMAL(5,2),
                
                -- Network metrics
                bytes_sent BIGINT,
                bytes_received BIGINT,
                connection_reused BOOLEAN,
                
                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE
            );
            
            -- Indexes for metrics table
            CREATE INDEX idx_metrics_page_id ON metrics(page_id);
            CREATE INDEX idx_metrics_session_id ON metrics(session_id);
            CREATE INDEX idx_metrics_total_time ON metrics(total_time);
            CREATE INDEX idx_metrics_created_at ON metrics(created_at);
            """,
            down_sql="""
            DROP TABLE IF EXISTS metrics CASCADE;
            """,
            dependencies=["010"]
        )
        
        # Migration 012: Create errors table for ErrorModel
        self.migrations["012"] = Migration.create(
            version="012",
            name="create_errors_table",
            up_sql="""
            -- Create errors table to match ErrorModel
            CREATE TABLE IF NOT EXISTS errors (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                page_id UUID REFERENCES pages(id) ON DELETE SET NULL,
                url TEXT,
                
                -- Error details
                error_type VARCHAR(50) NOT NULL,
                error_message TEXT NOT NULL,
                error_code VARCHAR(50),
                stack_trace TEXT,
                
                -- Context
                depth INTEGER,
                retry_count INTEGER DEFAULT 0,
                worker_id VARCHAR(50),
                
                -- Resolution
                is_resolved BOOLEAN DEFAULT FALSE,
                resolution_notes TEXT,
                
                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE
            );
            
            -- Indexes for errors table
            CREATE INDEX idx_errors_session_id ON errors(session_id);
            CREATE INDEX idx_errors_page_id ON errors(page_id);
            CREATE INDEX idx_errors_error_type ON errors(error_type);
            CREATE INDEX idx_errors_is_resolved ON errors(is_resolved);
            CREATE INDEX idx_errors_created_at ON errors(created_at);
            """,
            down_sql="""
            DROP TABLE IF EXISTS errors CASCADE;
            """,
            dependencies=["011"]
        )
        
        # Migration 013: Update links table to match LinkModel
        self.migrations["013"] = Migration.create(
            version="013",
            name="sync_links_with_models",
            up_sql="""
            -- Add missing fields to links table to match LinkModel
            ALTER TABLE links ADD COLUMN IF NOT EXISTS source_url TEXT;
            ALTER TABLE links ADD COLUMN IF NOT EXISTS link_title TEXT;
            ALTER TABLE links ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT TRUE;
            ALTER TABLE links ADD COLUMN IF NOT EXISTS discovery_depth INTEGER DEFAULT 0;
            ALTER TABLE links ADD COLUMN IF NOT EXISTS discovery_order INTEGER DEFAULT 0;
            ALTER TABLE links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
            
            -- Update existing data
            UPDATE links SET link_type = 'internal' WHERE link_type IS NULL;
            UPDATE links SET is_internal = (link_type = 'internal');
            
            -- Add indexes for new fields
            CREATE INDEX IF NOT EXISTS idx_links_source_url ON links(source_url);
            CREATE INDEX IF NOT EXISTS idx_links_is_internal ON links(is_internal);
            CREATE INDEX IF NOT EXISTS idx_links_discovery_depth ON links(discovery_depth);
            CREATE INDEX IF NOT EXISTS idx_links_updated_at ON links(updated_at);
            """,
            down_sql="""
            -- Remove added fields
            ALTER TABLE links DROP COLUMN IF EXISTS source_url;
            ALTER TABLE links DROP COLUMN IF EXISTS link_title;
            ALTER TABLE links DROP COLUMN IF EXISTS is_internal;
            ALTER TABLE links DROP COLUMN IF EXISTS discovery_depth;
            ALTER TABLE links DROP COLUMN IF EXISTS discovery_order;
            ALTER TABLE links DROP COLUMN IF EXISTS updated_at;
            
            -- Drop indexes
            DROP INDEX IF EXISTS idx_links_source_url;
            DROP INDEX IF EXISTS idx_links_is_internal;
            DROP INDEX IF EXISTS idx_links_discovery_depth;
            DROP INDEX IF EXISTS idx_links_updated_at;
            """,
            dependencies=["012"]
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
                    # Drop all tables in reverse dependency order
                    drop_tables = [
                        "error_events",
                        "system_metrics_timeseries", 
                        "session_metrics_timeseries",
                        "links",
                        "word_frequencies",
                        "pages",
                        "crawl_sessions",
                        "schema_migrations"
                    ]
                    
                    for table in drop_tables:
                        await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    
                    # Drop extensions
                    await conn.execute('DROP EXTENSION IF EXISTS "btree_gin"')
                    await conn.execute('DROP EXTENSION IF EXISTS "pg_trgm"')
                    await conn.execute('DROP EXTENSION IF EXISTS "uuid-ossp"')
                    
                    print("✓ Dropped all existing tables")
                    
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