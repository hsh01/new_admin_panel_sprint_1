import os
from .components.base import (
    INSTALLED_APPS,
    MIDDLEWARE,
    STATICFILES_DIRS,
)

DEBUG = os.environ.get('DEBUG', False) == 'True'

if DEBUG:
    INSTALLED_APPS += [
        "debug_toolbar",
    ]

    MIDDLEWARE += [
        'debug_toolbar.middleware.DebugToolbarMiddleware',
    ]

    INTERNAL_IPS = [
        "127.0.0.1",
    ]

    def _custom_show_toolbar(request):
        """Only show the debug toolbar to users with the superuser flag."""
        return DEBUG and request.user.is_superuser

    DEBUG_TOOLBAR_CONFIG = {
        'SHOW_TOOLBAR_CALLBACK':
            'config.local_settings._custom_show_toolbar',
    }

    STATICFILES_DIRS += ['debug_toolbar/static',]
