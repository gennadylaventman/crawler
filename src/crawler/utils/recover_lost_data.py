#!/usr/bin/env python3
"""
Data Recovery Tool for Web Crawler

This tool identifies and recovers lost session statistics from interrupted crawl sessions.
It recalculates session statistics based on actual data stored in the database.

Usage:
  python recover_lost_data.py                           # Recover all sessions with lost data
  python recover_lost_data.py --session-id <uuid>       # Recover specific session by ID
  python recover_lost_data.py --session-name <name>     # Recover specific session by name
"""
import asyncio
import asyncpg
import sys
import os
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add the src directory to Python path
sys.path.insert(0, 'src')

async def identify_sessions_with_lost_data(conn) -> List[Dict[str, Any]]:
    """Identify sessions where reported statistics don't match actual database content."""
    
    sessions = await conn.fetch('''
        SELECT id, name, status, total_pages_crawled, started_at
        FROM crawl_sessions
        ORDER BY started_at DESC
    ''')
    
    lost_data_sessions = []
    
    for session in sessions:
        # Check actual pages in database
        actual_pages = await conn.fetchval('''
            SELECT COUNT(*) FROM pages WHERE session_id = $1
        ''', session['id'])
        
        reported_pages = session['total_pages_crawled'] or 0
        
        if actual_pages > reported_pages:
            lost_data_sessions.append({
                'id': session['id'],
                'name': session['name'],
                'status': session['status'],
                'reported_pages': reported_pages,
                'actual_pages': actual_pages,
                'lost_pages': actual_pages - reported_pages,
                'started_at': session['started_at']
            })
    
    return lost_data_sessions

async def find_session_by_identifier(conn, session_id: Optional[str] = None, session_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Find a session by ID or name."""
    
    if session_id:
        # Find by session ID
        session = await conn.fetchrow('''
            SELECT id, name, status, total_pages_crawled, started_at
            FROM crawl_sessions 
            WHERE id = $1
        ''', session_id)
        
        if not session:
            print(f"❌ Session with ID '{session_id}' not found!")
            return None
            
    elif session_name:
        # Find by session name (get most recent if multiple)
        session = await conn.fetchrow('''
            SELECT id, name, status, total_pages_crawled, started_at
            FROM crawl_sessions 
            WHERE name = $1
            ORDER BY started_at DESC
            LIMIT 1
        ''', session_name)
        
        if not session:
            print(f"❌ Session with name '{session_name}' not found!")
            return None
    else:
        return None
    
    # Check if session has lost data
    actual_pages = await conn.fetchval('''
        SELECT COUNT(*) FROM pages WHERE session_id = $1
    ''', session['id'])
    
    reported_pages = session['total_pages_crawled'] or 0
    
    return {
        'id': session['id'],
        'name': session['name'],
        'status': session['status'],
        'reported_pages': reported_pages,
        'actual_pages': actual_pages,
        'lost_pages': actual_pages - reported_pages,
        'started_at': session['started_at']
    }

async def recover_session_statistics(conn, session_id: str) -> Dict[str, Any]:
    """Recover and recalculate session statistics based on actual database content."""
    
    print(f"🔄 Recovering session statistics for: {session_id}")
    
    # Get actual page count
    total_pages = await conn.fetchval('''
        SELECT COUNT(*) FROM pages WHERE session_id = $1
    ''', session_id)
    
    # Get successful pages count
    successful_pages = await conn.fetchval('''
        SELECT COUNT(*) FROM pages
        WHERE session_id = $1 AND status_code BETWEEN 200 AND 299
    ''', session_id)
    
    # Get failed pages count (for error_count)
    failed_pages = await conn.fetchval('''
        SELECT COUNT(*) FROM pages
        WHERE session_id = $1 AND (status_code < 200 OR status_code >= 400)
    ''', session_id)
    
    # Get total words found across all pages
    total_words = await conn.fetchval('''
        SELECT COALESCE(SUM(frequency), 0) FROM word_frequencies WHERE session_id = $1
    ''', session_id)
    
    # Get unique words count
    unique_words = await conn.fetchval('''
        SELECT COUNT(DISTINCT word) FROM word_frequencies WHERE session_id = $1
    ''', session_id)
    
    # Get word frequency count
    word_frequency_records = await conn.fetchval('''
        SELECT COUNT(*) FROM word_frequencies WHERE session_id = $1
    ''', session_id)
    
    # Calculate average page processing time
    avg_processing_time = await conn.fetchval('''
        SELECT AVG(total_page_time) FROM pages
        WHERE session_id = $1 AND total_page_time IS NOT NULL
    ''', session_id)
    
    # Get timing statistics
    timing_stats = await conn.fetchrow('''
        SELECT
            AVG(dns_lookup_time) as avg_dns_time,
            AVG(tcp_connect_time) as avg_tcp_time,
            AVG(server_response_time) as avg_response_time,
            AVG(html_parse_time) as avg_parse_time,
            AVG(text_extraction_time) as avg_extraction_time,
            AVG(word_counting_time) as avg_word_time,
            AVG(total_processing_time) as avg_processing_time,
            AVG(db_insert_time) as avg_db_time,
            AVG(total_page_time) as avg_total_time
        FROM pages
        WHERE session_id = $1
    ''', session_id)
    
    # Get session start and end times from actual page data
    page_times = await conn.fetchrow('''
        SELECT
            MIN(crawled_at) as first_page_time,
            MAX(crawled_at) as last_page_time
        FROM pages
        WHERE session_id = $1
    ''', session_id)
    
    recovery_stats = {
        'session_id': session_id,
        'total_pages_crawled': total_pages,
        'successful_pages': successful_pages,
        'failed_pages': failed_pages,
        'total_words': total_words,
        'unique_words': unique_words,
        'word_frequency_records': word_frequency_records,
        'avg_processing_time': float(avg_processing_time) if avg_processing_time else 0.0,
        'timing_stats': dict(timing_stats) if timing_stats else {},
        'first_page_time': page_times['first_page_time'] if page_times else None,
        'last_page_time': page_times['last_page_time'] if page_times else None
    }
    
    return recovery_stats

async def update_session_statistics(conn, session_id: str, stats: Dict[str, Any]) -> bool:
    """Update the session record with recovered statistics."""
    
    try:
        # Update the crawl_sessions table with recovered data
        # Only update columns that actually exist in the database
        await conn.execute('''
            UPDATE crawl_sessions
            SET
                total_pages_crawled = $2,
                total_words_found = $3,
                total_unique_words = $4,
                average_response_time = $5,
                error_count = $6,
                status = CASE
                    WHEN status = 'pending' AND $2 > 0 THEN 'completed'
                    ELSE status
                END
            WHERE id = $1
        ''',
            session_id,
            stats['total_pages_crawled'],
            stats.get('total_words', 0),
            stats.get('unique_words', 0),
            stats['avg_processing_time'],
            stats['failed_pages']
        )
        
        print(f"✅ Successfully updated session {session_id}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to update session {session_id}: {e}")
        return False

async def main():
    """Main recovery process with command line argument support."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Recover lost session statistics from web crawler database')
    parser.add_argument('--session-id', help='Specific session ID to recover')
    parser.add_argument('--session-name', help='Specific session name to recover')
    args = parser.parse_args()
    
    try:
        # Connect to database
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='webcrawler',
            user='crawler',
            password='crawler_password'
        )
        
        if args.session_id or args.session_name:
            # Recover specific session
            print(f"🔍 Looking for specific session...")
            session = await find_session_by_identifier(conn, args.session_id, args.session_name)
            
            if not session:
                await conn.close()
                return
            
            print(f"📋 Found session: {session['name']}")
            print(f"  ID: {session['id']}")
            print(f"  Status: {session['status']}")
            print(f"  Reported Pages: {session['reported_pages']}")
            print(f"  Actual Pages: {session['actual_pages']}")
            print(f"  Lost Pages: {session['lost_pages']}")
            print(f"  Started: {session['started_at']}")
            print()
            
            if session['lost_pages'] <= 0:
                print("✅ No data recovery needed - session statistics are consistent!")
                await conn.close()
                return
            
            print(f"🔄 Recovering session: {session['name']} ({session['lost_pages']} lost pages)")
            stats = await recover_session_statistics(conn, session['id'])
            
            print(f"📈 Recovery Summary:")
            print(f"  Total Pages: {stats['total_pages_crawled']}")
            print(f"  Successful: {stats['successful_pages']}")
            print(f"  Failed: {stats['failed_pages']}")
            print(f"  Total Words: {stats['total_words']}")
            print(f"  Unique Words: {stats['unique_words']}")
            print(f"  Word Frequency Records: {stats['word_frequency_records']}")
            print(f"  Avg Processing Time: {stats['avg_processing_time']:.3f}s")
            
            if await update_session_statistics(conn, session['id'], stats):
                print(f"🎉 Session recovery SUCCESSFUL!")
                print(f"✅ Recovered {stats['total_pages_crawled']} pages of data")
            else:
                print(f"❌ Session recovery FAILED!")
        
        else:
            # Recover all sessions with lost data
            print("🔍 Identifying sessions with lost data...")
            lost_data_sessions = await identify_sessions_with_lost_data(conn)
            
            if not lost_data_sessions:
                print("✅ No sessions with lost data found!")
                await conn.close()
                return
            
            print(f"\n📊 Found {len(lost_data_sessions)} sessions with lost data:")
            print("=" * 80)
            
            # Show all sessions with lost data
            for session in lost_data_sessions:
                print(f"Session: {session['name']}")
                print(f"  ID: {session['id']}")
                print(f"  Status: {session['status']}")
                print(f"  Reported Pages: {session['reported_pages']}")
                print(f"  Actual Pages: {session['actual_pages']}")
                print(f"  🚨 Lost Pages: {session['lost_pages']}")
                print(f"  Started: {session['started_at']}")
                print()
            
            # Process all sessions
            print(f"🔧 Processing all {len(lost_data_sessions)} sessions...")
            recovered_count = 0
            
            for session in lost_data_sessions:
                print(f"\n🔄 Processing session: {session['name']} ({session['lost_pages']} lost pages)")
                
                # Recover statistics
                stats = await recover_session_statistics(conn, session['id'])
                
                print(f"📈 Recovery Summary:")
                print(f"  Total Pages: {stats['total_pages_crawled']}")
                print(f"  Successful: {stats['successful_pages']}")
                print(f"  Failed: {stats['failed_pages']}")
                print(f"  Total Words: {stats['total_words']}")
                print(f"  Unique Words: {stats['unique_words']}")
                print(f"  Word Frequency Records: {stats['word_frequency_records']}")
                print(f"  Avg Processing Time: {stats['avg_processing_time']:.3f}s")
                
                # Update session
                if await update_session_statistics(conn, session['id'], stats):
                    recovered_count += 1
                    print(f"✅ Session recovery SUCCESSFUL!")
                else:
                    print(f"❌ Session recovery FAILED!")
            
            print(f"\n🎉 Recovery Complete!")
            print(f"✅ Successfully recovered {recovered_count} out of {len(lost_data_sessions)} sessions")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Recovery failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())