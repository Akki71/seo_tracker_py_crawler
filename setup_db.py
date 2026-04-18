#!/usr/bin/env python3
"""
setup_db.py — Interactive PostgreSQL database setup helper.
Run this once to create the database and verify connection.
Usage: python setup_db.py
"""
import os
import sys

# Load .env first
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    # Manual parse
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

print()
print("=" * 55)
print("  AquilTechLabs — PostgreSQL Database Setup")
print("=" * 55)

# ── Read connection details ────────────────────────────────────
print("\nDatabase connection settings (from .env):")
host     = os.environ.get("DB_HOST", "localhost")
port     = os.environ.get("DB_PORT", "5432")
user     = os.environ.get("DB_USER", "postgres")
password = os.environ.get("DB_PASSWORD", "")
dbname   = os.environ.get("DB_NAME", "seo_crawler")

print(f"  Host:     {host}")
print(f"  Port:     {port}")
print(f"  User:     {user}")
print(f"  Password: {'****' if password else 'NOT SET ⚠'}")
print(f"  Database: {dbname}")

if not password:
    print()
    print("⚠  DB_PASSWORD is not set in your .env file!")
    print("   Edit .env and add: DB_PASSWORD=yourpassword")
    print()
    manual = input("   Or enter password now (won't be saved): ").strip()
    if manual:
        password = manual
        os.environ["DB_PASSWORD"] = password
    else:
        print("   Exiting — set DB_PASSWORD in .env and re-run.")
        sys.exit(1)

# ── Test connection ────────────────────────────────────────────
print("\n[1] Testing connection to PostgreSQL server...")
try:
    import psycopg2
except ImportError:
    print("   ✗ psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Connect to postgres system DB first (to create our DB if needed)
try:
    conn = psycopg2.connect(
        host=host, port=int(port), user=user,
        password=password, dbname="postgres",
    )
    conn.autocommit = True
    print("   ✓ Connected to PostgreSQL server!")
except psycopg2.OperationalError as e:
    print(f"   ✗ Connection failed: {e}")
    print()
    print("   Troubleshooting:")
    print("   • Is PostgreSQL running? Check Windows Services → postgresql-x64-*")
    print("   • Is the password correct?")
    print(f"   • Can you reach {host}:{port}?")
    sys.exit(1)

# ── Create database if missing ─────────────────────────────────
print(f"\n[2] Checking if database '{dbname}' exists...")
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()

if exists:
    print(f"   ✓ Database '{dbname}' already exists")
else:
    print(f"   Creating database '{dbname}'...")
    with conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{dbname}"')
    print(f"   ✓ Database '{dbname}' created!")

conn.close()

# ── Init schema ────────────────────────────────────────────────
print(f"\n[3] Initializing schema in '{dbname}'...")
try:
    from db import init_db
    init_db()
    print("   ✓ All tables created successfully!")
except Exception as e:
    print(f"   ✗ Schema init failed: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# ── Verify ────────────────────────────────────────────────────
print(f"\n[4] Verifying tables...")
try:
    conn2 = psycopg2.connect(
        host=host, port=int(port), user=user, password=password, dbname=dbname
    )
    with conn2.cursor() as cur:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        tables = [r[0] for r in cur.fetchall()]
    conn2.close()
    print(f"   Tables found: {', '.join(tables)}")
    print(f"   ✓ {len(tables)} tables verified!")
except Exception as e:
    print(f"   ✗ Verification failed: {e}")

# ── Done ───────────────────────────────────────────────────────
print()
print("=" * 55)
print("  ✓ Database setup complete!")
print()
print("  Next steps:")
print("  1. Make sure your .env has all values set")
print("  2. Run: python startup.py")
print("  3. Open: http://localhost:8000/docs")
print("=" * 55)
print()