"""JupyterHub usage domain logic.

The modules here know about server-snapshot documents, sessions, and new users.
They take plain data and return plain data,
so that the command modules in :mod:`jupyterhub_monitoring.cli`
are left with argument handling and orchestration.
"""
