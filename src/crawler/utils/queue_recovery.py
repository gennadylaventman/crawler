"""
Queue recovery and cleanup utilities for persistent URL queues.
"""

import asyncio
import argparse
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from crawler.utils.config import get_config
from crawler.storage.database import DatabaseManager
from crawler.utils.exceptions import DatabaseError


class QueueRecoveryManager:
    """
    Manager for queue recovery and cleanup operations.
    Leverages existing DatabaseManager methods to avoid duplication.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def recover_all_interrupted_sessions(self, timeout_minutes: int = 5) -> Dict[str, int]:
        """Recover all interrupted sessions across the system."""
        try:
            async with self.db_manager.get_connection() as conn:
                # Find all sessions with stuck processing URLs
                stuck_sessions = await conn.fetch("""
                    SELECT DISTINCT session_id, COUNT(*) as stuck_count
                    FROM url_queue 
                    WHERE status = 'processing' 
                        AND updated_at < NOW() - INTERVAL '%s minutes'
                    GROUP BY session_id
                """, timeout_minutes)
                
                recovery_results = {}
                
                for session_row in stuck_sessions:
                    session_id = str(session_row['session_id'])
                    stuck_count = session_row['stuck_count']
                    
                    # Use existing DatabaseManager method
                    recovered = await self.db_manager.recover_interrupted_queue_urls(
                        session_id, timeout_minutes
                    )
                    
                    recovery_results[session_id] = {
                        'stuck_count': stuck_count,
                        'recovered_count': recovered
                    }
                    
                    print(f"Session {session_id}: Recovered {recovered}/{stuck_count} URLs")
                
                return recovery_results
                
        except Exception as e:
            raise DatabaseError(f"Failed to recover interrupted sessions: {e}")
    
    async def cleanup_all_old_entries(self, hours_old: int = 24) -> Dict[str, int]:
        """Clean up old completed/failed URLs across all sessions."""
        try:
            async with self.db_manager.get_connection() as conn:
                # Find sessions with old entries
                sessions_with_old_entries = await conn.fetch("""
                    SELECT session_id, COUNT(*) as old_count
                    FROM url_queue 
                    WHERE status IN ('completed', 'failed')
                        AND updated_at < NOW() - INTERVAL '%s hours'
                    GROUP BY session_id
                """, hours_old)
                
                cleanup_results = {}
                
                for session_row in sessions_with_old_entries:
                    session_id = str(session_row['session_id'])
                    old_count = session_row['old_count']
                    
                    # Use existing DatabaseManager method
                    cleaned = await self.db_manager.cleanup_old_queue_entries(
                        session_id, hours_old
                    )
                    
                    cleanup_results[session_id] = {
                        'old_count': old_count,
                        'cleaned_count': cleaned
                    }
                    
                    print(f"Session {session_id}: Cleaned {cleaned}/{old_count} old entries")
                
                return cleanup_results
                
        except Exception as e:
            raise DatabaseError(f"Failed to cleanup old queue entries: {e}")
    
    async def reset_failed_urls(self, session_id: str, max_attempts: int = 3) -> int:
        """Reset failed URLs back to pending for retry."""
        try:
            async with self.db_manager.get_connection() as conn:
                result = await conn.execute("""
                    UPDATE url_queue 
                    SET status = 'pending', attempts = 0, error_message = NULL, updated_at = NOW()
                    WHERE session_id = $1 
                        AND status = 'failed'
                        AND attempts <= $2
                """, session_id, max_attempts)
                
                # Extract count from result string
                reset_count = int(result.split()[-1]) if result else 0
                
                print(f"Reset {reset_count} failed URLs back to pending for session {session_id}")
                return reset_count
                
        except Exception as e:
            raise DatabaseError(f"Failed to reset failed URLs: {e}")
    
    async def get_detailed_session_summary(self, session_id: str) -> Dict[str, Any]:
        """Get detailed queue summary for a specific session with additional diagnostics."""
        try:
            # Use existing DatabaseManager method for basic stats
            basic_stats = await self.db_manager.get_queue_statistics(session_id)
            
            async with self.db_manager.get_connection() as conn:
                # Add detailed diagnostics not covered by basic stats
                
                # Get stuck processing URLs
                stuck_urls = await conn.fetch("""
                    SELECT url, attempts, updated_at, error_message
                    FROM url_queue 
                    WHERE session_id = $1 
                        AND status = 'processing'
                        AND updated_at < NOW() - INTERVAL '5 minutes'
                    ORDER BY updated_at
                    LIMIT 10
                """, session_id)
                
                # Get most failed URLs
                failed_urls = await conn.fetch("""
                    SELECT url, attempts, error_message, updated_at
                    FROM url_queue 
                    WHERE session_id = $1 
                        AND status = 'failed'
                    ORDER BY attempts DESC, updated_at DESC
                    LIMIT 10
                """, session_id)
                
                # Get timing statistics
                timing_stats = await conn.fetchrow("""
                    SELECT 
                        MIN(discovered_at) as first_discovered,
                        MAX(updated_at) as last_updated,
                        AVG(EXTRACT(EPOCH FROM (updated_at - discovered_at))) as avg_processing_time_seconds
                    FROM url_queue 
                    WHERE session_id = $1
                """, session_id)
                
                return {
                    'session_id': session_id,
                    'basic_statistics': basic_stats,
                    'timing_info': dict(timing_stats) if timing_stats else {},
                    'diagnostics': {
                        'stuck_processing_urls': [
                            {
                                'url': row['url'],
                                'attempts': row['attempts'],
                                'stuck_since': row['updated_at'],
                                'error_message': row['error_message']
                            } for row in stuck_urls
                        ],
                        'most_failed_urls': [
                            {
                                'url': row['url'],
                                'attempts': row['attempts'],
                                'error_message': row['error_message'],
                                'last_attempt': row['updated_at']
                            } for row in failed_urls
                        ]
                    }
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get detailed session summary: {e}")
    
    async def analyze_queue_bottlenecks(self) -> Dict[str, Any]:
        """Analyze system-wide queue bottlenecks and performance issues."""
        try:
            # Use existing DatabaseManager method for overall health
            health_metrics = await self.db_manager.get_queue_health_metrics()
            
            async with self.db_manager.get_connection() as conn:
                # Additional bottleneck analysis
                
                # Find domains with high failure rates
                domain_failures = await conn.fetch("""
                    SELECT 
                        CASE 
                            WHEN url ~ '^https?://([^/]+)' THEN 
                                substring(url from '^https?://([^/]+)')
                            ELSE 'unknown'
                        END as domain,
                        COUNT(*) as total_urls,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_urls,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END)::DECIMAL / COUNT(*) * 100 as failure_rate,
                        AVG(attempts) as avg_attempts
                    FROM url_queue 
                    WHERE updated_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY domain
                    HAVING COUNT(*) >= 10
                    ORDER BY failure_rate DESC, failed_urls DESC
                    LIMIT 10
                """)
                
                # Find sessions with processing bottlenecks
                slow_sessions = await conn.fetch("""
                    SELECT 
                        s.name as session_name,
                        s.id as session_id,
                        COUNT(q.*) as queue_size,
                        COUNT(CASE WHEN q.status = 'processing' THEN 1 END) as processing_count,
                        MIN(CASE WHEN q.status = 'processing' THEN q.updated_at END) as oldest_processing,
                        AVG(CASE WHEN q.status IN ('completed', 'failed') 
                            THEN EXTRACT(EPOCH FROM (q.updated_at - q.discovered_at)) END) as avg_completion_time
                    FROM crawl_sessions s
                    LEFT JOIN url_queue q ON s.id = q.session_id
                    WHERE s.status IN ('running', 'pending')
                    GROUP BY s.id, s.name
                    HAVING COUNT(CASE WHEN q.status = 'processing' AND q.updated_at < NOW() - INTERVAL '10 minutes' THEN 1 END) > 0
                    ORDER BY COUNT(CASE WHEN q.status = 'processing' AND q.updated_at < NOW() - INTERVAL '10 minutes' THEN 1 END) DESC
                    LIMIT 5
                """)
                
                return {
                    'timestamp': datetime.now().isoformat(),
                    'overall_health': health_metrics,
                    'bottleneck_analysis': {
                        'problematic_domains': [
                            {
                                'domain': row['domain'],
                                'total_urls': row['total_urls'],
                                'failed_urls': row['failed_urls'],
                                'failure_rate': float(row['failure_rate']),
                                'avg_attempts': float(row['avg_attempts'])
                            } for row in domain_failures
                        ],
                        'slow_sessions': [
                            {
                                'session_name': row['session_name'],
                                'session_id': str(row['session_id']),
                                'queue_size': row['queue_size'],
                                'processing_count': row['processing_count'],
                                'oldest_processing': row['oldest_processing'],
                                'avg_completion_time': float(row['avg_completion_time']) if row['avg_completion_time'] else None
                            } for row in slow_sessions
                        ]
                    }
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to analyze queue bottlenecks: {e}")


async def main():
    """Main CLI interface for queue recovery operations."""
    parser = argparse.ArgumentParser(description='Queue Recovery and Cleanup Utility')
    parser.add_argument('--config', default='development', help='Configuration environment')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Recovery command
    recover_parser = subparsers.add_parser('recover', help='Recover interrupted sessions')
    recover_parser.add_argument('--timeout', type=int, default=5, 
                               help='Timeout in minutes for stuck processing URLs')
    recover_parser.add_argument('--session-id', help='Specific session ID to recover')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old queue entries')
    cleanup_parser.add_argument('--hours', type=int, default=24,
                               help='Age in hours for entries to clean up')
    cleanup_parser.add_argument('--session-id', help='Specific session ID to clean up')
    
    # Health report command
    subparsers.add_parser('health', help='Generate queue health report')
    
    # Bottleneck analysis command
    subparsers.add_parser('analyze', help='Analyze queue bottlenecks and performance issues')
    
    # Session summary command
    summary_parser = subparsers.add_parser('summary', help='Get detailed session queue summary')
    summary_parser.add_argument('session_id', help='Session ID to summarize')
    
    # Reset failed URLs command
    reset_parser = subparsers.add_parser('reset-failed', help='Reset failed URLs to pending')
    reset_parser.add_argument('session_id', help='Session ID to reset failed URLs for')
    reset_parser.add_argument('--max-attempts', type=int, default=3,
                             help='Maximum attempts for URLs to reset')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize database manager
    config = get_config(args.config)
    db_manager = DatabaseManager(config.database)
    await db_manager.initialize(auto_migrate=False)
    
    recovery_manager = QueueRecoveryManager(db_manager)
    
    try:
        if args.command == 'recover':
            if args.session_id:
                recovered = await db_manager.recover_interrupted_queue_urls(
                    args.session_id, args.timeout
                )
                print(f"Recovered {recovered} URLs for session {args.session_id}")
            else:
                results = await recovery_manager.recover_all_interrupted_sessions(args.timeout)
                if isinstance(results, dict) and results:
                    total_recovered = sum(r.get('recovered_count', 0) if isinstance(r, dict) else 0 for r in results.values())
                    print(f"Recovered {total_recovered} URLs across {len(results)} sessions")
                else:
                    print(f"Recovery completed")
        
        elif args.command == 'cleanup':
            if args.session_id:
                cleaned = await db_manager.cleanup_old_queue_entries(
                    args.session_id, args.hours
                )
                print(f"Cleaned {cleaned} old entries for session {args.session_id}")
            else:
                results = await recovery_manager.cleanup_all_old_entries(args.hours)
                if isinstance(results, dict) and results:
                    total_cleaned = sum(r.get('cleaned_count', 0) if isinstance(r, dict) else 0 for r in results.values())
                    print(f"Cleaned {total_cleaned} old entries across {len(results)} sessions")
                else:
                    print("No old entries found to clean up")
        
        elif args.command == 'health':
            health_metrics = await db_manager.get_queue_health_metrics()
            
            print("=== Queue Health Report ===")
            print(f"Generated at: {datetime.now().isoformat()}")
            print()
            
            overall = health_metrics['overall_statistics']
            print("Overall Statistics:")
            print(f"  Total URLs: {overall.get('total_urls', 0)}")
            print(f"  Pending: {overall.get('pending_urls', 0)}")
            print(f"  Processing: {overall.get('processing_urls', 0)}")
            print(f"  Completed: {overall.get('completed_urls', 0)}")
            print(f"  Failed: {overall.get('failed_urls', 0)}")
            print(f"  Active Sessions: {overall.get('active_sessions', 0)}")
            print()
            
            if health_metrics['stuck_sessions']:
                print("Sessions with Stuck URLs:")
                for session in health_metrics['stuck_sessions']:
                    print(f"  Session {session['session_id'][:8]}...")
                    print(f"    Stuck URLs: {session['stuck_urls']}")
                    print(f"    Oldest stuck: {session['oldest_stuck']}")
                    print()
        
        elif args.command == 'analyze':
            analysis = await recovery_manager.analyze_queue_bottlenecks()
            
            print("=== Queue Bottleneck Analysis ===")
            print(f"Generated at: {analysis['timestamp']}")
            print()
            
            if analysis['bottleneck_analysis']['problematic_domains']:
                print("Problematic Domains (High Failure Rate):")
                for domain in analysis['bottleneck_analysis']['problematic_domains']:
                    print(f"  {domain['domain']}")
                    print(f"    Total URLs: {domain['total_urls']}")
                    print(f"    Failed: {domain['failed_urls']} ({domain['failure_rate']:.1f}%)")
                    print(f"    Avg Attempts: {domain['avg_attempts']:.1f}")
                    print()
            
            if analysis['bottleneck_analysis']['slow_sessions']:
                print("Sessions with Processing Issues:")
                for session in analysis['bottleneck_analysis']['slow_sessions']:
                    print(f"  {session['session_name']} ({session['session_id'][:8]}...)")
                    print(f"    Queue Size: {session['queue_size']}")
                    print(f"    Processing: {session['processing_count']}")
                    if session['avg_completion_time']:
                        print(f"    Avg Completion Time: {session['avg_completion_time']:.1f}s")
                    print()
        
        elif args.command == 'summary':
            summary = await recovery_manager.get_detailed_session_summary(args.session_id)
            
            print(f"=== Detailed Queue Summary for Session {args.session_id} ===")
            
            # Basic statistics from DatabaseManager
            basic_stats = summary['basic_statistics']
            if 'status_counts' in basic_stats:
                print("Status Distribution:")
                for status, count in basic_stats['status_counts'].items():
                    print(f"  {status}: {count}")
                print()
            
            # Timing information
            timing = summary['timing_info']
            if timing:
                print("Timing Information:")
                if timing.get('first_discovered'):
                    print(f"  First URL discovered: {timing['first_discovered']}")
                if timing.get('last_updated'):
                    print(f"  Last activity: {timing['last_updated']}")
                if timing.get('avg_processing_time_seconds'):
                    print(f"  Avg processing time: {timing['avg_processing_time_seconds']:.1f}s")
                print()
            
            # Diagnostics
            diagnostics = summary['diagnostics']
            if diagnostics['stuck_processing_urls']:
                print("Stuck Processing URLs:")
                for url_info in diagnostics['stuck_processing_urls']:
                    print(f"  {url_info['url']}")
                    print(f"    Attempts: {url_info['attempts']}")
                    print(f"    Stuck since: {url_info['stuck_since']}")
                    print()
        
        elif args.command == 'reset-failed':
            reset_count = await recovery_manager.reset_failed_urls(
                args.session_id, args.max_attempts
            )
            print(f"Reset {reset_count} failed URLs back to pending")
    
    finally:
        await db_manager.close()


if __name__ == '__main__':
    asyncio.run(main())