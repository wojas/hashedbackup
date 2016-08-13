# v0 probably has no users other than the original author, but just in case..
UPGRADE_TO_REPOSITORY_V1 = """
It looks like your backup repository is a version 0 one, which is no longer
supported.  You need to upgrade this one manually to version 1.

Version 1 uses a flatter object bucket structure (objects/aa/hashedfile
instead of objects/aa/bb/hashedfile) for better performance and reduced
complexity, and requires these buckets to be created at init time, to reduce
the number of mkdir commands we need to send to the server at backup time.

You can execute the following shell commands from inside your repository
to convert it into a v1 repository:

    cd objects/

    # Migrate from two-level buckets to the new single level
    for dir in *; do echo $dir; cd $dir && mv */* . && cd ..; done
    for i in {0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}; do rmdir $i?/??; done

    # Precreate the 256 buckets, in case they do not all exist yet
    mkdir {0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}

    cd ..

    # Create the repository config file
    echo '{"version": 1}' > hashedbackup.json

Future versions of hashedbackup will continue to support version 1, or offer a
way to upgrade the repository format automatically to spare you this hassle.

Thanks for using hashedbackup in such an early stage!
"""
