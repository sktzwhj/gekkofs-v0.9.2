Scripts
=======

The GekkoFS package includes several scripts that automate/simplify common
tasks. This section describes such scripts, which can be classified as follows:

.. contents::
    :local:
    :depth: 1
    :backlinks: none

Dependency Management Scripts
-----------------------------

GekkoFS provides two scripts for managing dependencies which can be found in
the :code:`PROJECT_ROOT/scripts` directory: :code:`dl_dep.sh` and
:code:`compile_dep.sh`. As their names suggest, the :code:`dl_dep.sh` script
is in charge of downloading any dependencies required by GekkoFS to build or
run correctly, while :code:`compile_dep.sh` is responsible for installing them
once downloaded.

Since dependencies may change or need to be configured differently depending
on the specifics of the particular GekkoFS build, both scripts rely on
:code:`configuration profiles` which define a set of related software
packages which should be downloaded and installed for a specific GekkoFS
version and/or configuration. To illustrate this, let's take a look at the
contents of the :code:`default` profile for GekkoFS version :code:`0.9.2`:

.. code-block:: console

    $ dl_dep.sh -l default:0.9.2
    Configuration profiles for '0.9.2':

    * default:0.9.2 (/home/user/gekkofs/source/scripts/profiles/0.9.2/default.specs)

      All dependencies

        lz4: 1.9.3
        capstone: 4.0.2
        json-c: 0.15-20200726
        libfabric: HEAD@v1.13.2
        mercury: v2.1.0
        argobots: 1.1
        margo: v0.9.6
        rocksdb: 6.26.1
        syscall_intercept: 2c8765fa292bc9c28a22624c528580d54658813d
        date: e7e1482087f58913b80a20b04d5c58d9d6d90155



Downloading dependencies
########################

Dependencies defined in a profile can be downloaded using the :code:`dl_dep.sh`
script and the :code:`-p` option. As shown below, profile names follow the
:code:`PROFILE_NAME[:VERSION_TAG]` naming convention, where :code:`PROFILE_NAME`
serves to uniquely identify a particular configuration (e.g. for a specific
supercomputer) followed by an optional :code:`VERSION_TAG`.

.. code-block:: console

    $ ./dl_dep.sh -p default:0.9.2 /home/user/gfks/deps
    Destination path is set to  "/tmp/foo"
    Profile name: default
    Profile version: 0.9.2
    ------------------------------------
    Downloaded 'https://github.com/lz4/lz4/archive/v1.9.3.tar.gz' to 'lz4'
    Downloaded 'https://github.com/json-c/json-c/archive/json-c-0.15-20200726.tar.gz' to 'json-c'
    Cloned 'https://github.com/pmem/syscall_intercept.git' to 'syscall_intercept' with commit '[2c8765fa292bc9c28a22624c528580d54658813d]' and flags ''
    Checking patch include/libsyscall_intercept_hook_point.h...
    Checking patch src/intercept.c...
    Hunk #2 succeeded at 700 (offset 31 lines).
    Hunk #3 succeeded at 730 (offset 31 lines).
    Checking patch test/test_clone_thread_preload.c...
    Applied patch include/libsyscall_intercept_hook_point.h cleanly.
    Applied patch src/intercept.c cleanly.
    Applied patch test/test_clone_thread_preload.c cleanly.
    Cloned 'https://github.com/HowardHinnant/date.git' to 'date' with commit '[e7e1482087f58913b80a20b04d5c58d9d6d90155]' and flags ''
    Downloaded 'https://github.com/pmodels/argobots/archive/v1.1.tar.gz' to 'argobots'
    Cloned 'https://github.com/mochi-hpc/mochi-margo' to 'margo' with commit '[v0.9.6]' and flags ''
    Downloaded 'https://github.com/aquynh/capstone/archive/4.0.2.tar.gz' to 'capstone'
    Downloaded 'https://github.com/facebook/rocksdb/archive/v6.26.1.tar.gz' to 'rocksdb'
    Cloned 'https://github.com/ofiwg/libfabric.git' to 'libfabric' with commit '[HEAD]' and flags '--branch=v1.13.2'
    Cloned 'https://github.com/mercury-hpc/mercury' to 'mercury' with commit '[v2.1.0]' and flags '--recurse-submodules'
    Done

It is also possible to download a specific dependency with the :code:`-d`
option. In this case, dependency names follow the
:code:`DEPENDENCY_NAME[@PROFILE_NAME[:VERSION_TAG]]` naming convention.

.. code-block:: console

   $ ./dl_dep.sh -d mercury@default:0.9.2 /home/user/gfks/deps
   Destination path is set to  "/tmp/foo"
   Profile name: default
   Profile version: 0.9.2
   ------------------------------------
   Cloned 'https://github.com/mercury-hpc/mercury' to 'mercury' with commit '[v2.1.0]' and flags '--recurse-submodules'
   Done

.. warning::

    Note that :code:`PROFILE_NAME` and :code:`VERSION_TAG` can be optional
    in most script invocations. If :code:`PROFILE_NAME` is left unspecified,
    the scripts will assume that the :code:`default` profile was selected.
    Similarly, if a :code:`VERSION_NAME` is not provided, the scripts will
    assume that the :code:`latest` version should be used.

Installing dependencies
########################

Once dependencies in a configuration profile have been downloaded to a
certain directory (e.g. :code:`/home/user/gkfs/deps`), the
:code:`compile_dep.sh` script can be used to install them.

.. code-block:: console

    $ ./compile_dep.sh -p default:0.9.2 /home/user/gkfs/deps /home/user/gkfs/install -j8
    CORES = 8 (default)
    Sources download path = /tmp/foo
    Installation path = /tmp/bar
    Profile name: default
    Profile version: 0.9.2
    ------------------------------------
    ######## Installing:  lz4 ###############################
    ...

    ######## Installing:  capstone ###############################
    ...

    ######## Installing:  json-c ###############################
    ...

    ######## Installing:  libfabric ###############################
    ...

    ######## Installing:  mercury ###############################
    ...

    ######## Installing:  argobots ###############################
    ...

    ######## Installing:  margo ###############################
    ...

    ######## Installing:  rocksdb ###############################
    ...

    ######## Installing:  syscall_intercept ###############################
    ...

    ######## Installing:  date ###############################
    ...

    Done
