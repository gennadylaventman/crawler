"""
Performance profiling utilities for the web crawler system.

This module provides comprehensive performance monitoring, profiling,
and analysis tools to identify bottlenecks and optimize crawler performance.
"""

import time
import psutil
import threading
import asyncio
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from collections import defaultdict, deque
from contextlib import contextmanager, asynccontextmanager
from functools import wraps
import cProfile
import pstats
import io
from pathlib import Path

from crawler.utils.exceptions import CrawlerError
from crawler.monitoring.logger import get_logger


@dataclass
class ProfileData:
    """Container for profiling data."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    memory_start: Optional[float] = None
    memory_end: Optional[float] = None
    memory_peak: Optional[float] = None
    cpu_percent: Optional[float] = None
    thread_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def finalize(self) -> None:
        """Finalize profiling data."""
        if self.end_time is None:
            self.end_time = time.perf_counter()
        
        if self.duration is None:
            self.duration = self.end_time - self.start_time
        
        if self.memory_start is not None and self.memory_end is not None:
            self.memory_peak = max(self.memory_start, self.memory_end)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'duration': self.duration,
            'memory_start_mb': self.memory_start,
            'memory_end_mb': self.memory_end,
            'memory_peak_mb': self.memory_peak,
            'cpu_percent': self.cpu_percent,
            'thread_id': self.thread_id,
            'metadata': self.metadata
        }


class PerformanceProfiler:
    """
    Performance profiler for tracking execution times and resource usage.
    """
    
    def __init__(self, enable_memory_tracking: bool = True, enable_cpu_tracking: bool = True):
        self.enable_memory_tracking = enable_memory_tracking
        self.enable_cpu_tracking = enable_cpu_tracking
        self.profiles: Dict[str, List[ProfileData]] = defaultdict(list)
        self.active_profiles: Dict[str, ProfileData] = {}
        self._lock = threading.Lock()
        self.logger = get_logger('profiler')
        
        # Process handle for resource monitoring
        self._process = psutil.Process() if (enable_memory_tracking or enable_cpu_tracking) else None
    
    def start_profile(self, name: str, **metadata) -> str:
        """
        Start profiling an operation.
        
        Args:
            name: Profile name
            **metadata: Additional metadata
            
        Returns:
            Profile ID
        """
        profile_id = f"{name}_{threading.get_ident()}_{time.time()}"
        
        profile_data = ProfileData(
            name=name,
            start_time=time.perf_counter(),
            thread_id=threading.get_ident(),
            metadata=metadata
        )
        
        # Capture initial resource usage
        if self._process:
            try:
                if self.enable_memory_tracking:
                    memory_info = self._process.memory_info()
                    profile_data.memory_start = memory_info.rss / 1024 / 1024  # MB
                
                if self.enable_cpu_tracking:
                    profile_data.cpu_percent = self._process.cpu_percent()
            except Exception as e:
                self.logger.warning(f"Failed to capture initial resource usage: {e}")
        
        with self._lock:
            self.active_profiles[profile_id] = profile_data
        
        return profile_id
    
    def end_profile(self, profile_id: str) -> Optional[ProfileData]:
        """
        End profiling an operation.
        
        Args:
            profile_id: Profile ID returned by start_profile
            
        Returns:
            ProfileData or None if profile not found
        """
        with self._lock:
            profile_data = self.active_profiles.pop(profile_id, None)
        
        if profile_data is None:
            self.logger.warning(f"Profile not found: {profile_id}")
            return None
        
        profile_data.end_time = time.perf_counter()
        
        # Capture final resource usage
        if self._process:
            try:
                if self.enable_memory_tracking:
                    memory_info = self._process.memory_info()
                    profile_data.memory_end = memory_info.rss / 1024 / 1024  # MB
                
                if self.enable_cpu_tracking:
                    # Get average CPU usage during the operation
                    current_cpu = self._process.cpu_percent()
                    if profile_data.cpu_percent is not None:
                        profile_data.cpu_percent = (profile_data.cpu_percent + current_cpu) / 2
                    else:
                        profile_data.cpu_percent = current_cpu
            except Exception as e:
                self.logger.warning(f"Failed to capture final resource usage: {e}")
        
        profile_data.finalize()
        
        # Store completed profile
        with self._lock:
            self.profiles[profile_data.name].append(profile_data)
        
        return profile_data
    
    @contextmanager
    def profile(self, name: str, **metadata):
        """
        Context manager for profiling operations.
        
        Args:
            name: Profile name
            **metadata: Additional metadata
        """
        profile_id = self.start_profile(name, **metadata)
        try:
            yield profile_id
        finally:
            self.end_profile(profile_id)
    
    @asynccontextmanager
    async def async_profile(self, name: str, **metadata):
        """
        Async context manager for profiling operations.
        
        Args:
            name: Profile name
            **metadata: Additional metadata
        """
        profile_id = self.start_profile(name, **metadata)
        try:
            yield profile_id
        finally:
            self.end_profile(profile_id)
    
    def get_profile_stats(self, name: str) -> Dict[str, Any]:
        """
        Get statistics for a specific profile.
        
        Args:
            name: Profile name
            
        Returns:
            Dictionary of statistics
        """
        with self._lock:
            profiles = self.profiles.get(name, [])
        
        if not profiles:
            return {}
        
        durations = [p.duration for p in profiles if p.duration is not None]
        memory_usage = [p.memory_peak for p in profiles if p.memory_peak is not None]
        cpu_usage = [p.cpu_percent for p in profiles if p.cpu_percent is not None]
        
        stats = {
            'name': name,
            'count': len(profiles),
            'total_duration': sum(durations),
            'average_duration': sum(durations) / len(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
        }
        
        if memory_usage:
            stats.update({
                'average_memory_mb': sum(memory_usage) / len(memory_usage),
                'peak_memory_mb': max(memory_usage),
                'min_memory_mb': min(memory_usage)
            })
        
        if cpu_usage:
            stats.update({
                'average_cpu_percent': sum(cpu_usage) / len(cpu_usage),
                'peak_cpu_percent': max(cpu_usage),
                'min_cpu_percent': min(cpu_usage)
            })
        
        return stats
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all profiles."""
        with self._lock:
            profile_names = list(self.profiles.keys())
        
        return {name: self.get_profile_stats(name) for name in profile_names}
    
    def clear_profiles(self, name: Optional[str] = None) -> None:
        """
        Clear profile data.
        
        Args:
            name: Specific profile name to clear, or None to clear all
        """
        with self._lock:
            if name:
                self.profiles.pop(name, None)
            else:
                self.profiles.clear()
    
    def export_profiles(self, filepath: Union[str, Path]) -> None:
        """
        Export profile data to JSON file.
        
        Args:
            filepath: Output file path
        """
        import json
        
        data = {}
        with self._lock:
            for name, profiles in self.profiles.items():
                data[name] = [profile.to_dict() for profile in profiles]
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)


class FunctionProfiler:
    """
    Decorator-based function profiler.
    """
    
    def __init__(self, profiler: PerformanceProfiler):
        self.profiler = profiler
    
    def __call__(self, name: Optional[str] = None, **metadata):
        """
        Decorator to profile function execution.
        
        Args:
            name: Profile name (defaults to function name)
            **metadata: Additional metadata
        """
        def decorator(func: Callable):
            profile_name = name or f"{func.__module__}.{func.__name__}"
            
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    async with self.profiler.async_profile(profile_name, **metadata):
                        return await func(*args, **kwargs)
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    with self.profiler.profile(profile_name, **metadata):
                        return func(*args, **kwargs)
                return sync_wrapper
        
        return decorator


class SystemResourceMonitor:
    """
    System resource monitoring for the crawler.
    """
    
    def __init__(self, collection_interval: float = 1.0, history_size: int = 1000):
        self.collection_interval = collection_interval
        self.history_size = history_size
        self.running = False
        self._monitor_task: Optional[asyncio.Task] = None
        
        # Resource history
        self.cpu_history: deque = deque(maxlen=history_size)
        self.memory_history: deque = deque(maxlen=history_size)
        self.disk_history: deque = deque(maxlen=history_size)
        self.network_history: deque = deque(maxlen=history_size)
        
        self.logger = get_logger('resource_monitor')
    
    async def start_monitoring(self) -> None:
        """Start resource monitoring."""
        if self.running:
            return
        
        self.running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        self.logger.info("Started system resource monitoring")
    
    async def stop_monitoring(self) -> None:
        """Stop resource monitoring."""
        if not self.running:
            return
        
        self.running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Stopped system resource monitoring")
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self.running:
            try:
                timestamp = time.time()
                
                # Collect CPU metrics
                cpu_percent = psutil.cpu_percent(interval=None)
                cpu_count = psutil.cpu_count()
                load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)
                
                self.cpu_history.append({
                    'timestamp': timestamp,
                    'cpu_percent': cpu_percent,
                    'cpu_count': cpu_count,
                    'load_1min': load_avg[0],
                    'load_5min': load_avg[1],
                    'load_15min': load_avg[2]
                })
                
                # Collect memory metrics
                memory = psutil.virtual_memory()
                self.memory_history.append({
                    'timestamp': timestamp,
                    'total_mb': memory.total / 1024 / 1024,
                    'available_mb': memory.available / 1024 / 1024,
                    'used_mb': memory.used / 1024 / 1024,
                    'percent': memory.percent
                })
                
                # Collect disk metrics
                disk = psutil.disk_usage('/')
                disk_io = psutil.disk_io_counters()
                self.disk_history.append({
                    'timestamp': timestamp,
                    'total_gb': disk.total / 1024 / 1024 / 1024,
                    'used_gb': disk.used / 1024 / 1024 / 1024,
                    'free_gb': disk.free / 1024 / 1024 / 1024,
                    'percent': (disk.used / disk.total) * 100,
                    'read_mb': disk_io.read_bytes / 1024 / 1024 if disk_io else 0,
                    'write_mb': disk_io.write_bytes / 1024 / 1024 if disk_io else 0
                })
                
                # Collect network metrics
                network = psutil.net_io_counters()
                self.network_history.append({
                    'timestamp': timestamp,
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv,
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                })
                
                await asyncio.sleep(self.collection_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in resource monitoring: {e}")
                await asyncio.sleep(self.collection_interval)
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current system statistics."""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            return {
                'cpu': {
                    'percent': cpu_percent,
                    'count': psutil.cpu_count()
                },
                'memory': {
                    'total_mb': memory.total / 1024 / 1024,
                    'available_mb': memory.available / 1024 / 1024,
                    'used_mb': memory.used / 1024 / 1024,
                    'percent': memory.percent
                },
                'disk': {
                    'total_gb': disk.total / 1024 / 1024 / 1024,
                    'used_gb': disk.used / 1024 / 1024 / 1024,
                    'free_gb': disk.free / 1024 / 1024 / 1024,
                    'percent': (disk.used / disk.total) * 100
                },
                'network': {
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv,
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                }
            }
        except Exception as e:
            self.logger.error(f"Failed to get current stats: {e}")
            return {}
    
    def get_history_summary(self, metric_type: str, duration_minutes: int = 10) -> Dict[str, Any]:
        """
        Get summary statistics for a metric type over a time period.
        
        Args:
            metric_type: Type of metric ('cpu', 'memory', 'disk', 'network')
            duration_minutes: Duration in minutes to analyze
            
        Returns:
            Summary statistics
        """
        history_map = {
            'cpu': self.cpu_history,
            'memory': self.memory_history,
            'disk': self.disk_history,
            'network': self.network_history
        }
        
        history = history_map.get(metric_type)
        if not history:
            return {}
        
        # Filter by time window
        cutoff_time = time.time() - (duration_minutes * 60)
        recent_data = [item for item in history if item['timestamp'] >= cutoff_time]
        
        if not recent_data:
            return {}
        
        # Calculate summary based on metric type
        if metric_type == 'cpu':
            cpu_values = [item['cpu_percent'] for item in recent_data]
            return {
                'average_cpu_percent': sum(cpu_values) / len(cpu_values),
                'max_cpu_percent': max(cpu_values),
                'min_cpu_percent': min(cpu_values),
                'samples': len(cpu_values)
            }
        elif metric_type == 'memory':
            memory_values = [item['percent'] for item in recent_data]
            return {
                'average_memory_percent': sum(memory_values) / len(memory_values),
                'max_memory_percent': max(memory_values),
                'min_memory_percent': min(memory_values),
                'samples': len(memory_values)
            }
        
        return {'samples': len(recent_data)}


class CodeProfiler:
    """
    Code profiler using cProfile for detailed function-level analysis.
    """
    
    def __init__(self, output_dir: Union[str, Path] = "profiles"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.profiler: Optional[cProfile.Profile] = None
        self.logger = get_logger('code_profiler')
    
    def start_profiling(self) -> None:
        """Start code profiling."""
        if self.profiler is not None:
            self.logger.warning("Profiler already running")
            return
        
        self.profiler = cProfile.Profile()
        self.profiler.enable()
        self.logger.info("Started code profiling")
    
    def stop_profiling(self, filename: Optional[str] = None) -> Optional[str]:
        """
        Stop code profiling and save results.
        
        Args:
            filename: Output filename (auto-generated if None)
            
        Returns:
            Path to saved profile file
        """
        if self.profiler is None:
            self.logger.warning("No active profiler to stop")
            return None
        
        self.profiler.disable()
        
        if filename is None:
            timestamp = int(time.time())
            filename = f"profile_{timestamp}.prof"
        
        output_path = self.output_dir / filename
        self.profiler.dump_stats(str(output_path))
        
        self.profiler = None
        self.logger.info(f"Saved profile to {output_path}")
        
        return str(output_path)
    
    def analyze_profile(self, profile_path: str, top_functions: int = 20) -> Dict[str, Any]:
        """
        Analyze a saved profile file.
        
        Args:
            profile_path: Path to profile file
            top_functions: Number of top functions to include
            
        Returns:
            Analysis results
        """
        try:
            stats = pstats.Stats(profile_path)
            stats.sort_stats('cumulative')
            
            # Capture stats output
            output = io.StringIO()
            stats.print_stats(top_functions, file=output)
            stats_text = output.getvalue()
            
            # Get function statistics
            function_stats = []
            for func, (cc, nc, tt, ct, callers) in stats.stats.items():
                function_stats.append({
                    'function': f"{func[0]}:{func[1]}({func[2]})",
                    'call_count': cc,
                    'total_time': tt,
                    'cumulative_time': ct,
                    'per_call_time': tt / cc if cc > 0 else 0
                })
            
            # Sort by cumulative time
            function_stats.sort(key=lambda x: x['cumulative_time'], reverse=True)
            
            return {
                'total_calls': stats.total_calls,
                'total_time': stats.total_tt,
                'top_functions': function_stats[:top_functions],
                'stats_text': stats_text
            }
            
        except Exception as e:
            self.logger.error(f"Failed to analyze profile: {e}")
            return {}
    
    @contextmanager
    def profile_context(self, filename: Optional[str] = None):
        """
        Context manager for profiling code blocks.
        
        Args:
            filename: Output filename
        """
        self.start_profiling()
        try:
            yield
        finally:
            self.stop_profiling(filename)


# Global profiler instances
_performance_profiler: Optional[PerformanceProfiler] = None


def get_performance_profiler() -> PerformanceProfiler:
    """Get global performance profiler instance."""
    global _performance_profiler
    if _performance_profiler is None:
        _performance_profiler = PerformanceProfiler()
    return _performance_profiler


@asynccontextmanager
async def async_profile_operation(name: str, **metadata):
    """Async context manager for profiling operations."""
    profiler = get_performance_profiler()
    async with profiler.async_profile(name, **metadata):
        yield