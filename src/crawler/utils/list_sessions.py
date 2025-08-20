#!/usr/bin/env python3
"""
Simple script to list all crawl sessions with data analysis
"""
import asyncio
import asyncpg
import os

async def list_all_sessions():
    try:
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='webcrawler',
            user='crawler',
            password='crawler_password'
        )
        
        # Get ALL sessions sorted by start time
        sessions = await conn.fetch('''
            SELECT id, name, status, total_pages_crawled, started_at
            FROM crawl_sessions 
            ORDER BY started_at ASC
        ''')
        
        print('=== ALL CRAWL SESSIONS (chronological order) ===')
        print()
        
        for i, session in enumerate(sessions, 1):
            # Check actual pages in database for each session
            actual_pages = await conn.fetchval('''
                SELECT COUNT(*) FROM pages WHERE session_id = $1
            ''', session['id'])
            
            reported_pages = session['total_pages_crawled'] or 0
            data_discrepancy = actual_pages - reported_pages
            
            print(f'{i}. Session: {session["name"]}')
            print(f'   ID: {session["id"]}')
            print(f'   Status: {session["status"]}')
            print(f'   Started: {session["started_at"]}')
            print(f'   Reported Pages: {reported_pages}')
            print(f'   ACTUAL Pages in DB: {actual_pages}')
            
            if data_discrepancy > 0:
                print(f'   🚨 DATA LOSS: {data_discrepancy} pages missing from session stats!')
            elif data_discrepancy < 0:
                print(f'   ⚠️  OVER-REPORTED: Session claims {abs(data_discrepancy)} more pages than exist')
            else:
                print(f'   ✅ Data consistent')
            
            print()
        
        await conn.close()
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(list_all_sessions())