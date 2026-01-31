"""
Configuration Management

Loads environment variables from .env file and provides configuration access.
"""

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    # If python-dotenv is not installed, we'll just use os.getenv
    def load_dotenv(path: Optional[str] = None) -> bool:
        return False


def load_env_file(env_path: Optional[str] = None) -> bool:
    """
    Load environment variables from .env file.
    
    Args:
        env_path: Path to .env file (defaults to .env in project root)
        
    Returns:
        True if .env file was loaded, False otherwise
    """
    if env_path is None:
        # Look for .env in project root
        project_root = Path(__file__).parent.parent
        env_path = project_root / ".env"
    
    if isinstance(env_path, Path):
        env_path = str(env_path)
    
    return load_dotenv(env_path)


# Load .env file automatically when module is imported
load_env_file()


class Config:
    """Configuration class for accessing environment variables."""
    
    # Confluence Configuration
    @staticmethod
    def get_confluence_url() -> str:
        return os.getenv("CONFLUENCE_URL", "https://slicepay.atlassian.net/wiki")
    
    @staticmethod
    def get_confluence_username() -> Optional[str]:
        return os.getenv("CONFLUENCE_USERNAME")
    
    @staticmethod
    def get_confluence_api_token() -> Optional[str]:
        return os.getenv("CONFLUENCE_API_TOKEN")
    
    @staticmethod
    def get_confluence_space_key() -> Optional[str]:
        return os.getenv("CONFLUENCE_SPACE_KEY", "HOR")
    
    # Jira Configuration
    @staticmethod
    def get_jira_url() -> str:
        return os.getenv("JIRA_URL", "https://slicepay.atlassian.net")
    
    @staticmethod
    def get_jira_username() -> Optional[str]:
        return os.getenv("JIRA_USERNAME")
    
    @staticmethod
    def get_jira_api_token() -> Optional[str]:
        return os.getenv("JIRA_API_TOKEN")
    
    # Slack Configuration
    @staticmethod
    def get_slack_bot_token() -> Optional[str]:
        return os.getenv("SLACK_BOT_TOKEN")
    
    @staticmethod
    def get_slack_default_channel() -> Optional[str]:
        return os.getenv("SLACK_DEFAULT_CHANNEL", "#general")
    
    @staticmethod
    def get_slack_workspace_name() -> Optional[str]:
        return os.getenv("SLACK_WORKSPACE_NAME")
    
    @staticmethod
    def get_slack_xoxc_token() -> Optional[str]:
        return os.getenv("SLACK_MCP_XOXC_TOKEN")
    
    @staticmethod
    def get_slack_xoxd_token() -> Optional[str]:
        return os.getenv("SLACK_MCP_XOXD_TOKEN")
    
    # Database Configuration
    @staticmethod
    def get_database_url() -> Optional[str]:
        return os.getenv("DATABASE_URL")
    
    # Vector DB Configuration
    @staticmethod
    def get_vector_db_path() -> str:
        return os.getenv("VECTOR_DB_PATH", "./data/vector_db")
    
    @staticmethod
    def get_vector_db_index_path() -> str:
        return os.getenv("VECTOR_DB_INDEX_PATH", "./data/vector_index")
    
    # Knowledge Base Paths
    @staticmethod
    def get_knowledge_base_path() -> str:
        return os.getenv("KNOWLEDGE_BASE_PATH", "metadata/knowledge_base.json")
    
    @staticmethod
    def get_knowledge_register_path() -> str:
        return os.getenv("KNOWLEDGE_REGISTER_PATH", "metadata/knowledge_register.json")
    
    @staticmethod
    def get_product_index_path() -> str:
        return os.getenv("PRODUCT_INDEX_PATH", "metadata/product_index.json")
    
    # LLM Configuration
    @staticmethod
    def get_openai_api_key() -> Optional[str]:
        return os.getenv("OPENAI_API_KEY")
    
    @staticmethod
    def get_anthropic_api_key() -> Optional[str]:
        return os.getenv("ANTHROPIC_API_KEY")
    
    # Other Configuration
    @staticmethod
    def get_log_level() -> str:
        return os.getenv("LOG_LEVEL", "INFO")
    
    @staticmethod
    def is_debug() -> bool:
        return os.getenv("DEBUG", "false").lower() == "true"
    
    @staticmethod
    def validate_confluence_config() -> tuple[bool, Optional[str]]:
        """Validate Confluence configuration."""
        url = Config.get_confluence_url()
        username = Config.get_confluence_username()
        token = Config.get_confluence_api_token()
        
        if not username:
            return False, "CONFLUENCE_USERNAME is not set"
        if not token:
            return False, "CONFLUENCE_API_TOKEN is not set"
        if not url:
            return False, "CONFLUENCE_URL is not set"
        
        return True, None
    
    @staticmethod
    def validate_slack_config() -> tuple[bool, Optional[str]]:
        """Validate Slack configuration."""
        token = Config.get_slack_bot_token()
        
        if not token:
            return False, "SLACK_BOT_TOKEN is not set"
        
        return True, None





