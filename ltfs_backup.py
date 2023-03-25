import xattr
import os
import sys
import shutil
from dns.rdataclass import NONE
from tqdm import tqdm
import time
import progressbar

class LinkCopyItem():
    def __init__(self, source, destination):
        '''
        Stores a source and a destination and provides facilities for copying.
        
        '''
        self.source=source
        self.destination=destination
    
    def add_destination(self, directory):
        self.destination=FileItem(self.source.filename, directory)
    
    def create_link(self):
        source=self.source.filename_full
        destination=self.destination.filename_full
        os.link(source, destination)
        
    def copy(self):
        '''
        Copies from source to destination
        '''
        source_fname=self.source.filename_full
        dest_fname=self.destination.filename_full
        shutil.copy2(source_fname, dest_fname)
        # Set extended attributes
        for key in xattr.list(source_fname):
            
            src_attribute=xattr.get(source_fname)
            print(key, src_attribute)
            xattr.set(dest_fname, key, src_attribute)

class FileItem():
    def __init__(self, filename, directory, update_properties=False):
        '''
        Stores the name, size and mtime of a file and allows for comparing files.
        
        :param directory:
        :param filename:
        :property size:
        '''
        
        self.filename_full=os.path.join(directory, filename)
        self.filename=filename
        self.directory=directory
        self.my_size=None 
        self.my_mtime=None
        if update_properties:
            self.update_properties()
    
    @property
    def size(self):
        if self.my_size is None:
            self.get_properties()
        return self.my_size
        
    @property
    def mtime(self):
        if self.my_mtime is None:
            self.get_properties()
        return self.my_mtime
        
    def update_properties(self):
        '''
        Updates the size and mtime of the file
        '''
        self.my_size  = os.path.getsize(self.filename_full)
        self.my_mtime = os.path.getmtime(self.filename_full)
        
    def compare(self, other_file):
        '''
        Compares the item to another file item.
        '''
        
        if self.size != other_file.size:
            return False
        elif self.filename != other_file.filename:
            return False
        elif self.mtime != other_file.mtime:
            return False
        else:
            return True
        
    
def find_files(directory):
    '''
    Searches the given directory for files.
    
    :param directory:
    '''
    files_found={}
    dir_list=[]
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_name=os.path.join(root, file)
            # Save the relative path in relation to the main directory
            file_name=os.path.relpath(file_name, directory)
            file_item=FileItem(file_name, directory, update_properties=True)
            #file_item.get_properties()
            files_found[file_name]=file_item
        
        for d in dirs:
            dir_list.append(d)
    
    return files_found, dir_list
        
class LtfsBackup():
    
    def __init__(self, source, referece, destination):
        '''
        This is a simple incremental backup tool for ltfs.
        
        After the backup, the data in "destination" will match the "source". 
        Files present in "reference" will be hard linked to "reference" and not copied.   
        
        Theory of operation: 
        - We create a list of files including xargs for  
            - the source 
            - the reference 
            - the destination
              
        - We create 2 lists:
            - copy
            - hard link
        
        - In the following, a file is termed present if the following properties match between source an the location of comparison:
            - File name
            - File size
            - Modification time
        
        - Files be allocated to the lists as follows:
            - Files present in destination: ignored
            - Files present on reference: hard link
            - All other files: copy
        
        - A file is created in destination to mark the ongoing backup
        - Hard links are created
        - Files are copied
        - A file is created in destination to mark the complete backup
        
        '''
        self.source=source
        self.referece=referece
        self.destination=destination
        
        print('Creating list of source files')
        self.files_source, self.dirs_source=find_files(source)
        print('Creating list of reference files')
        self.files_reference, self.dirs_referece=find_files(referece)
        print('Creating list of destination files')
        self.files_destination, self.dirs_destination=find_files(destination)
        print('Assembling copy and hardlink list')
        self._create_lists()
    
    def _create_lists(self):
        '''
        Creates the copy and hardlink list
        '''
        
        self.hardlink_list=[]
        self.copy_list=[]
        reference_keys=self.files_reference.keys()
        destination_keys=self.files_destination.keys()
        for file_name, source_object in self.files_source.items():
            # Check if the file name is present at the destination
            if file_name in destination_keys:
                destination_object=self.files_destination[file_name]
                # Compare the two files.
                if source_object.compare(destination_object):
                    # Ignore the file if it is present at the destination
                    continue
                
            # Check if the file is present in the reference 
            if file_name in reference_keys:
                reference_object=self.files_reference[file_name]
                # Compare the two files.
                if source_object.compare(reference_object):
                    # If the items match, add the reference file to the hardlink list
                    # We will need to add the destination directory later on.
                    copy_item=LinkCopyItem(reference_object, None)
                    self.hardlink_list.append(copy_item)
                    continue
            # If we came that far, we need to copy the file.
            self.copy_list.append(LinkCopyItem(source_object, None))
        
                
    def create_directories(self):
        '''
        Creates the directory structure at the destination
        :TODO: Implement copying of access rights, owner, etc. 
        '''
        
        # We use a set for checking the presence of destination files.
        dest_dirs=set(self.dirs_destination)
        for d in self.dirs_source:
            if not d in dest_dirs:
                dest_dir=os.path.join(self.destination, d)
                if not os.path.exists(dest_dir):
                    os.mkdir(dest_dir)
        
    def copy_files(self):
        '''
        Copies all files present in the copy list
        '''
        # Compute the size of the files we need to copy.
        print(f'Computig size')
        
        size=0
        for copy_item in self.copy_list:
            size+=copy_item.source.size
        
        scaling=1024**3
        size_gb=size/scaling
        print(f'Will copy {size_gb:.2f} GB')
        
        widgets = ['Copying: ', progressbar.AnimatedMarker()]
        bar = progressbar.ProgressBar(max_value=size_gb, widgets=widgets).start()
        size_copied=0
        my_time=time.time()
        #size_copied_old=0
        for file in self.copy_list:
            file.add_destination(self.destination)
            file.copy()
            s = copy_item.source.size
            # Don't bother updating the progress bar for files smaller than 1MB
            if s < 1E6:
                continue
            size_copied += s
            t=time.time()
            # Update the progress bar every 0.1s-
            if t + 0.1 >  my_time:
                my_time
                bar.update(size_copied/scaling)
        
    def create_hardlinks(self):
        '''
        Creates hardlinks
        '''
        for file in self.hardlink_list:
            file.add_destination(self.destination)
            file.create_link()

    
def main():


    if len(sys.argv) !=3:
        print('Invalid argument.')
        print('ltfs_backup.py source reference destination')
        
    source=sys.argv[1]
    reference=sys.argv[2]
    destination=sys.argv[3]

    ltb=LtfsBackup(source, reference, destination)
    print('Creating directories')
    ltb.create_directories()
    print('Copying files')
    ltb.copy_files()
    print('Creating Hard links')
    ltb.create_hardlinks()
    
    print("Done!")
    
    
if __name__ == '__main__':
    main()
    
