"""Main CLI entry point for MGST package."""

import click
from pathlib import Path

from .filter import filter_cmd
from .database import db


@click.group()
@click.version_option()
@click.pass_context
def main(ctx):
    """MGST - Mikunn Galactic Search Tool

    Elite Dangerous galaxy analysis toolkit with flexible JSON pattern matching
    and database construction utilities.
    """
    ctx.ensure_object(dict)


# Add subcommands
main.add_command(filter_cmd, name='filter')
main.add_command(db, name='db')


@main.command()
def version():
    """Show version information."""
    from ..__version__ import __version__
    click.echo(f"mgst version {__version__}")


if __name__ == '__main__':
    main()