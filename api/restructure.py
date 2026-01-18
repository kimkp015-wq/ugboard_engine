import os
import shutil

def restructure_project():
    # Define source and target directories
    source_dir = "ugboard_engine"
    files_to_move = ["main.py", "requirements.txt", "runtime.txt"]
    folders_to_move = ["api", "data", "scripts"]
    
    print("Restructuring project for Render deployment...")
    
    # Check if source exists
    if not os.path.exists(source_dir):
        print(f"❌ Source directory '{source_dir}' not found!")
        return
    
    # Move files
    for file in files_to_move:
        source = os.path.join(source_dir, file)
        if os.path.exists(source):
            shutil.move(source, file)
            print(f"✅ Moved: {source} -> {file}")
        else:
            print(f"⚠️  Not found: {file}")
    
    # Move folders
    for folder in folders_to_move:
        source = os.path.join(source_dir, folder)
        if os.path.exists(source):
            # Check if destination already exists
            if os.path.exists(folder):
                # Merge contents
                for item in os.listdir(source):
                    s = os.path.join(source, item)
                    d = os.path.join(folder, item)
                    if os.path.isdir(s):
                        shutil.copytree(s, d, dirs_exist_ok=True)
                    else:
                        shutil.copy2(s, d)
                print(f"✅ Merged: {folder}")
            else:
                shutil.move(source, folder)
                print(f"✅ Moved: {source} -> {folder}")
        else:
            print(f"⚠️  Not found: {folder}")
    
    # Remove empty source directory
    try:
        os.rmdir(source_dir)
        print(f"✅ Removed empty directory: {source_dir}")
    except OSError:
        print(f"⚠️  Could not remove {source_dir} (not empty)")
    
    print("\n✅ Restructuring complete!")
    print("New structure:")
    os.system("ls -la")

if __name__ == "__main__":
    restructure_project()
