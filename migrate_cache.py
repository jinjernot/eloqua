"""
Migrate contact cache from JSON to compressed GZIP format
"""
from core.rest.fetch_data import load_contact_cache, save_contact_cache
import os

print("="*60)
print("Contact Cache Migration")
print("="*60)

print("\nLoading existing cache...")
cache = load_contact_cache()

print(f"\nCache Statistics:")
print(f"  Total contacts: {len(cache):,}")

# Check file sizes
old_path = "data/contact_cache.json"
new_path = "data/contact_cache.json.gz"

if os.path.exists(old_path):
    old_size_mb = os.path.getsize(old_path) / (1024 * 1024)
    print(f"\nOld cache (JSON):")
    print(f"  Size: {old_size_mb:.2f} MB")
    
if os.path.exists(new_path):
    new_size_mb = os.path.getsize(new_path) / (1024 * 1024)
    print(f"\nNew cache (GZIP):")
    print(f"  Size: {new_size_mb:.2f} MB")
    
    if os.path.exists(old_path):
        reduction = ((old_size_mb - new_size_mb) / old_size_mb) * 100
        print(f"\n✓ Compression achieved: {reduction:.1f}% reduction")
        print(f"  Space saved: {old_size_mb - new_size_mb:.2f} MB")
        print(f"\nYou can now safely delete: {old_path}")

print("\n" + "="*60)
print("Migration complete!")
print("="*60)
