"""Central Flask extensions initialization.

Decouples SQLAlchemy and LoginManager instances from application factory and
feature blueprints to prevent circular imports.
"""

from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Please sign in to continue.'
login_manager.login_message_category = 'info'
