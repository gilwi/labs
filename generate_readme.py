import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
README_NAME = "README.md"

def find_lab_readmes(root_dir):
    entries = []
    for entry in os.listdir(root_dir):
        lab_path = os.path.join(root_dir, entry)
        readme_path = os.path.join(lab_path, README_NAME)
        if os.path.isdir(lab_path) and os.path.isfile(readme_path):
            relative_path = os.path.relpath(readme_path, root_dir)
            entries.append((entry, relative_path))
    return entries

def write_root_readme(entries, output_path):
    with open(output_path, 'w') as f:
        f.write("# Labs Index\n\n")
        for name, path in sorted(entries):
            f.write(f"- [{name}]({path})\n")

if __name__ == "__main__":
    labs = find_lab_readmes(ROOT_DIR)
    write_root_readme(labs, os.path.join(ROOT_DIR, README_NAME))
    print(f"✅ Generated root README with {len(labs)} labs.")

