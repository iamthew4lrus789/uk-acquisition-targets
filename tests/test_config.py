"""
Test Pydantic configuration management
"""
import pytest
import sys
from pathlib import Path
import tempfile
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_config_import_works():
    """Test that config module can be imported"""
    from src.config import settings, Settings
    assert settings is not None
    assert isinstance(settings, Settings)


def test_config_default_values():
    """Test that default configuration values are set correctly"""
    from src.config import settings
    
    assert settings.APP_NAME == "Companies House Data Pipeline"
    assert settings.APP_VERSION == "1.0.0"
    assert settings.ENVIRONMENT == "development"
    assert settings.DEBUG is False
    assert settings.WEB_PORT == 8000
    assert settings.MAX_RESULTS == 10000


def test_config_env_override():
    """Test that environment variables override defaults"""
    from src.config import Settings
    
    # Test with environment variable override
    os.environ['APP_NAME'] = 'Test App'
    os.environ['DEBUG'] = 'true'
    os.environ['WEB_PORT'] = '9000'
    
    test_settings = Settings()
    assert test_settings.APP_NAME == 'Test App'
    assert test_settings.DEBUG is True
    assert test_settings.WEB_PORT == 9000
    
    # Clean up
    del os.environ['APP_NAME']
    del os.environ['DEBUG']
    del os.environ['WEB_PORT']


def test_config_env_file():
    """Test that .env file loading works"""
    from src.config import Settings
    
    # Create temporary .env file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write("""
APP_NAME=Test From Env File
DEBUG=true
WEB_PORT=9999
        """)
        env_file = f.name
    
    try:
        # Test with custom env file
        test_settings = Settings(_env_file=env_file)
        assert test_settings.APP_NAME == 'Test From Env File'
        assert test_settings.DEBUG is True
        assert test_settings.WEB_PORT == 9999
    finally:
        # Clean up
        os.unlink(env_file)


def test_config_validation():
    """Test that Pydantic validation works"""
    from pydantic import ValidationError
    from src.config import Settings
    
    # Test invalid port
    with pytest.raises(ValidationError):
        Settings(WEB_PORT="not_a_number")
    
    # Test invalid rate limit
    with pytest.raises(ValidationError):
        Settings(RATE_LIMIT=123)  # Should be string


def test_config_path_types():
    """Test that Path types work correctly"""
    from src.config import settings
    
    assert isinstance(settings.PROCESSED_DIR, Path)
    assert isinstance(settings.TEMP_DIR, Path)
    assert str(settings.PROCESSED_DIR) == "processed"


def test_config_list_types():
    """Test that list types work correctly"""
    from src.config import settings
    
    assert isinstance(settings.CORS_ALLOW_ORIGINS, list)
    assert "*" in settings.CORS_ALLOW_ORIGINS
    assert isinstance(settings.CORS_ALLOW_METHODS, list)
    assert isinstance(settings.CORS_ALLOW_HEADERS, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
