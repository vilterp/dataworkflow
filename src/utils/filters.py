"""Template filters for Flask"""
from datetime import datetime, timezone


def timeago_filter(dt):
    """Convert a datetime to a relative time string (e.g., '2 hours ago')"""
    if not dt:
        return ''

    now = datetime.now(timezone.utc)
    # Ensure dt is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    diff = now - dt
    seconds = diff.total_seconds()

    if seconds < 60:
        return 'just now'
    elif seconds < 3600:  # Less than 1 hour
        minutes = int(seconds / 60)
        return f'{minutes} minute{"s" if minutes != 1 else ""} ago'
    elif seconds < 86400:  # Less than 1 day
        hours = int(seconds / 3600)
        return f'{hours} hour{"s" if hours != 1 else ""} ago'
    elif seconds < 2592000:  # Less than 30 days
        days = int(seconds / 86400)
        return f'{days} day{"s" if days != 1 else ""} ago'
    elif seconds < 31536000:  # Less than 1 year
        months = int(seconds / 2592000)
        return f'{months} month{"s" if months != 1 else ""} ago'
    else:
        years = int(seconds / 31536000)
        return f'{years} year{"s" if years != 1 else ""} ago'
