import os


def build_file_mappings(local_paths, remote_base_path):
    file_mappings = {}
    for local_path in local_paths:
        # Normalize and extract parts
        path_parts = os.path.normpath(local_path).split(os.sep)
        file_name = path_parts[-1]
        sub_folder = path_parts[-2]  # Assuming the subfolder is right before the file name in the path
        
        if sub_folder.lower() in ['html', 'plots', 'thumbnails']:  # Expected subfolders
            remote_path = os.path.join(remote_base_path, sub_folder, file_name)
        else:
            # Default to putting it directly under base if unexpected directory
            remote_path = os.path.join(remote_base_path, file_name)

        file_mappings[local_path] = remote_path.replace('\\', '/')

    return file_mappings