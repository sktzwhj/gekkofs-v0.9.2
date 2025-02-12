/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS' POSIX interface.

  GekkoFS' POSIX interface is free software: you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the License,
  or (at your option) any later version.

  GekkoFS' POSIX interface is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with GekkoFS' POSIX interface.  If not, see
  <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: LGPL-3.0-or-later
*/

#include <config.hpp>
#include <client/preload.hpp>
#include <client/preload_util.hpp>
#include <client/logging.hpp>
#include <client/gkfs_functions.hpp>
#include <client/rpc/forward_metadata.hpp>
#include <client/rpc/forward_data.hpp>
#include <client/open_dir.hpp>

#include <common/path_util.hpp>

extern "C" {
#include <dirent.h> // used for file types in the getdents{,64}() functions
#include <linux/kernel.h> // used for definition of alignment macros
#include <sys/statfs.h>
#include <sys/statvfs.h>
}

using namespace std;

/*
 * Macro used within getdents{,64} functions.
 * __ALIGN_KERNEL defined in linux/kernel.h
 */
#define ALIGN(x, a) __ALIGN_KERNEL((x), (a))

/*
 * linux_dirent is used in getdents() but is privately defined in the linux
 * kernel: fs/readdir.c.
 */
struct linux_dirent {
    unsigned long d_ino;
    unsigned long d_off;
    unsigned short d_reclen;
    char d_name[1];
};
/*
 * linux_dirent64 is used in getdents64() and defined in the linux kernel:
 * include/linux/dirent.h. However, it is not part of the kernel-headers and
 * cannot be imported.
 */
struct linux_dirent64 {
    uint64_t d_ino;
    int64_t d_off;
    unsigned short d_reclen;
    unsigned char d_type;
    char d_name[1]; // originally `char d_name[0]` in kernel, but ISO C++
                    // forbids zero-size array 'd_name'
};

struct dirent_extended {
    size_t size;
    time_t ctime;
    unsigned short d_reclen;
    unsigned char d_type;
    char d_name[1]; // originally `char d_name[0]` in kernel, but ISO C++
                    // forbids zero-size array 'd_name'
};


namespace {

/**
 * Checks if metadata for parent directory exists (can be disabled with
 * GKFS_CREATE_CHECK_PARENTS). errno may be set
 * @param path
 * @return 0 on success, -1 on failure
 */
int
check_parent_dir(const std::string& path) {
#if GKFS_CREATE_CHECK_PARENTS
    auto p_comp = gkfs::path::dirname(path);
    auto md = gkfs::utils::get_metadata(p_comp);
    if(!md) {
        if(errno == ENOENT) {
            LOG(DEBUG, "Parent component does not exist: '{}'", p_comp);
        } else {
            LOG(ERROR, "Failed to get metadata for parent component '{}': {}",
                path, strerror(errno));
        }
        return -1;
    }
    if(!S_ISDIR(md->mode())) {
        LOG(DEBUG, "Parent component is not a directory: '{}'", p_comp);
        errno = ENOTDIR;
        return -1;
    }
#endif // GKFS_CREATE_CHECK_PARENTS
    return 0;
}
} // namespace

namespace gkfs::syscall {

/**
 * gkfs wrapper for open() system calls
 * errno may be set
 * @param path
 * @param mode
 * @param flags
 * @return 0 on success, -1 on failure
 */
int
gkfs_open(const std::string& path, mode_t mode, int flags) {

    if(flags & O_PATH) {
        LOG(ERROR, "`O_PATH` flag is not supported");
        errno = ENOTSUP;
        return -1;
    }

    // metadata object filled during create or stat
    gkfs::metadata::Metadata md{};
    if(flags & O_CREAT) {
        if(flags & O_DIRECTORY) {
            LOG(ERROR, "O_DIRECTORY use with O_CREAT. NOT SUPPORTED");
            errno = ENOTSUP;
            return -1;
        }
        // no access check required here. If one is using our FS they have the
        // permissions.
        auto err = gkfs_create(path, mode | S_IFREG);
        if(err) {
            if(errno == EEXIST) {
                // file exists, O_CREAT was set
                if(flags & O_EXCL) {
                    // File exists and O_EXCL & O_CREAT was set
                    return -1;
                }
                // file exists, O_CREAT was set O_EXCL wasnt, so function does
                // not fail this case is actually undefined as per `man 2 open`
                auto md_ = gkfs::utils::get_metadata(path);
                if(!md_) {
                    LOG(ERROR,
                        "Could not get metadata after creating file '{}': '{}'",
                        path, strerror(errno));
                    return -1;
                }
                md = *md_;
#ifdef HAS_RENAME
                // This is an old file that was renamed which we do not open
                if(md.blocks() == -1) {
                    LOG(DEBUG,
                        "This file was renamed and we do not open. path '{}'",
                        path);
                    return -1;
                }
#endif // HAS_RENAME
            } else {
                LOG(ERROR, "Error creating file: '{}'", strerror(errno));
                return -1;
            }
        } else {
            // file was successfully created. Add to filemap
            return CTX->file_map()->add(
                    std::make_shared<gkfs::filemap::OpenFile>(path, flags));
        }
    } else {
        auto md_ = gkfs::utils::get_metadata(path);
        if(!md_) {
            if(errno != ENOENT) {
                LOG(ERROR, "Error stating existing file '{}'", path);
            }
            // file doesn't exist and O_CREAT was not set
            return -1;
        }
        md = *md_;
    }


#ifdef HAS_SYMLINKS
    if(md.is_link()) {
        if(flags & O_NOFOLLOW) {
            LOG(WARNING, "Symlink found and O_NOFOLLOW flag was specified");
            errno = ELOOP;
            return -1;
        }
        return gkfs_open(md.target_path(), mode, flags);
    }
#ifdef HAS_RENAME
    auto new_path = path;
    if(md.blocks() == -1) {
        // This is an old file that was renamed and essentially no longer exists
        errno = ENOENT;
        return -1;
    } else {
        if(!md.target_path().empty()) {
            // get renamed path from target and retrieve metadata from it
            auto md_ = gkfs::utils::get_metadata(md.target_path());
            new_path = md.target_path();
            while(!md_.value().target_path().empty()) {
                new_path = md_.value().target_path();
                md_ = gkfs::utils::get_metadata(md_.value().target_path(),
                                                false);
                if(!md_) {
                    return -1;
                }
            }
            md = *md_;
            if(S_ISDIR(md.mode())) {
                return gkfs_opendir(new_path);
            }

            /*** Regular file exists ***/
            assert(S_ISREG(md.mode()));

            if((flags & O_TRUNC) && ((flags & O_RDWR) || (flags & O_WRONLY))) {
                if(gkfs_truncate(new_path, md.size(), 0)) {
                    LOG(ERROR, "Error truncating file");
                    return -1;
                }
            }

            return CTX->file_map()->add(
                    std::make_shared<gkfs::filemap::OpenFile>(new_path, flags));
        }
    }
#endif // HAS_RENAME
#endif // HAS_SYMLINKS
    if(S_ISDIR(md.mode())) {
        return gkfs_opendir(path);
    }

    /*** Regular file exists ***/
    assert(S_ISREG(md.mode()));

    if((flags & O_TRUNC) && ((flags & O_RDWR) || (flags & O_WRONLY))) {
        if(gkfs_truncate(path, md.size(), 0)) {
            LOG(ERROR, "Error truncating file");
            return -1;
        }
    }

    return CTX->file_map()->add(
            std::make_shared<gkfs::filemap::OpenFile>(path, flags));
}

/**
 * Wrapper function for file/directory creation
 * errno may be set
 * @param path
 * @param mode
 * @return 0 on success, -1 on failure
 */
int
gkfs_create(const std::string& path, mode_t mode) {

    // file type must be set
    switch(mode & S_IFMT) {
        case 0:
            mode |= S_IFREG;
            break;
        case S_IFREG: // intentionally fall-through
        case S_IFDIR:
            break;
        case S_IFCHR: // intentionally fall-through
        case S_IFBLK:
        case S_IFIFO:
        case S_IFSOCK:
            LOG(WARNING, "Unsupported node type");
            errno = ENOTSUP;
            return -1;
        default:
            LOG(WARNING, "Unrecognized node type");
            errno = EINVAL;
            return -1;
    }

    if(check_parent_dir(path)) {
        return -1;
    }
    // Write to all replicas, at least one need to success
    bool success = false;
    for(auto copy = 0; copy < CTX->get_replicas() + 1; copy++) {
        auto err = gkfs::rpc::forward_create(path, mode, copy);
        if(err) {
            errno = err;
        } else {
            success = true;
            errno = 0;
        }
    }
    if(!success) {
        return -1;
    }

    return 0;
}

/**
 * gkfs wrapper for unlink() system calls
 * errno may be set
 * @param path
 * @return 0 on success, -1 on failure
 */
int
gkfs_remove(const std::string& path) {
    auto md = gkfs::utils::get_metadata(path);
    if(!md) {
        return -1;
    }

    if(S_ISDIR(md->mode())) {
        LOG(ERROR, "Cannot remove directory '{}'", path);
        errno = EISDIR;
        return -1;
    }
#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
    if(md.value().blocks() == -1) {
        errno = ENOENT;
        return -1;
    } else {
        if(!md.value().target_path().empty()) {
            auto md_ = gkfs::utils::get_metadata(md.value().target_path());
            std::string new_path = md.value().target_path();
            while(!md.value().target_path().empty()) {
                new_path = md.value().target_path();
                md = gkfs::utils::get_metadata(md.value().target_path(), false);
                if(!md) {
                    return -1;
                }
            }
            auto err = gkfs::rpc::forward_remove(new_path, CTX->get_replicas());
            if(err) {
                errno = err;
                return -1;
            }
        }
    }
#endif // HAS_RENAME
#endif // HAS_SYMLINKS

    auto err = gkfs::rpc::forward_remove(path, CTX->get_replicas());
    if(err) {
        errno = err;
        return -1;
    }
    return 0;
}

/**
 * gkfs wrapper for access() system calls
 * errno may be set
 * @param path
 * @param mask
 * @param follow_links
 * @return 0 on success, -1 on failure
 */
int
gkfs_access(const std::string& path, const int mask, bool follow_links) {
    auto md = gkfs::utils::get_metadata(path, follow_links);
    if(!md) {
        LOG(DEBUG, "File does not exist '{}'", path);
        return -1;
    }
#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
    LOG(DEBUG, "Checking for renamed file '{}'", path);
    if(md.value().blocks() == -1) {
        errno = ENOENT;
        LOG(DEBUG, "File exist but it is renamed '{}'", path);
        return -1;

    } else {
        while(!md.value().target_path().empty()) {
            LOG(DEBUG, "File exist but it is renamed '{} -> {}'", path,
                md.value().target_path());
            md = gkfs::utils::get_metadata(md.value().target_path(), false);
            if(!md) {
                LOG(DEBUG, "File does not exist but it is renamed '{} -> {}'",
                    path, md.value().target_path());
                return -1;
            }
        }
    }
#endif // HAS_RENAME
#endif // HAS_SYMLINKS
    return 0;
}

#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
/**
 * gkfs wrapper for rename() system calls
 * errno may be set
 * We use blocks to determine if the file is a renamed file.
 * If the file is re-renamed (a->b->a) a recovers the block of b
 * and we delete b.
 * There is no support for replication in rename
 * @param old_path
 * @param new_path
 * @return 0 on success, -1 on failure
 */
int
gkfs_rename(const string& old_path, const string& new_path) {
    auto md_old = gkfs::utils::get_metadata(old_path, false);

    // if the file is not found, or it is a renamed one cancel.
    if(!md_old || md_old.value().blocks() == -1) {
        return -1;
    }
    auto md_new = gkfs::utils::get_metadata(new_path, false);
    if(md_new) {
        // the new file exists... check for circular...
        if(md_new.value().blocks() == -1 &&
           md_old.value().target_path() == new_path) {
            // the new file is a renamed file, so we need to get the metadata of
            // the original file.
            LOG(DEBUG, "Destroying Circular Rename '{}' --> '{}'", old_path,
                new_path);
            gkfs::metadata::MetadentryUpdateFlags flags{};
            flags.atime = false;
            flags.mtime = false;
            flags.ctime = false;
            flags.blocks = true;
            flags.mode = false;
            flags.size = false;
            flags.uid = false;
            flags.gid = false;
            flags.link_count = false;
            md_old.value().blocks(0);
            md_old.value().target_path("");

            auto err = gkfs::rpc::forward_update_metadentry(
                    new_path, md_old.value(), flags, 0);

            if(err) {
                errno = err;
                return -1;
            }
            // Delete old file
            err = gkfs::rpc::forward_remove(old_path, CTX->get_replicas());
            if(err) {
                errno = err;
                return -1;
            }
            return 0;
        }
        return -1;
    }

    auto err = gkfs::rpc::forward_rename(old_path, new_path, md_old.value());
    if(err) {
        errno = err;
        return -1;
    }

    return 0;
}
#endif
#endif

/**
 * gkfs wrapper for stat() system calls
 * errno may be set
 * @param path
 * @param buf
 * @param follow_links
 * @return 0 on success, -1 on failure
 */
int
gkfs_stat(const string& path, struct stat* buf, bool follow_links) {
    auto md = gkfs::utils::get_metadata(path, follow_links);
    if(!md) {
        return -1;
    }
#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
    if(md.value().blocks() == -1) {
        errno = ENOENT;
        return -1;
    } else {
        while(!md.value().target_path().empty()) {
            md = gkfs::utils::get_metadata(md.value().target_path(), false);
            if(!md) {
                return -1;
            }
        }
    }
#endif
#endif
    gkfs::utils::metadata_to_stat(path, *md, *buf);
    return 0;
}

#ifdef STATX_TYPE

/**
 * gkfs wrapper for statx() system calls
 * errno may be set
 * @param dirfs
 * @param path
 * @param flags
 * @param mask
 * @param buf
 * @param follow_links
 * @return 0 on success, -1 on failure
 */
int
gkfs_statx(int dirfs, const std::string& path, int flags, unsigned int mask,
           struct statx* buf, bool follow_links) {
    auto md = gkfs::utils::get_metadata(path, follow_links);

    if(!md) {
        return -1;
    }
#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
    if(md.value().blocks() == -1) {
        errno = ENOENT;
        return -1;
    } else {
        while(!md.value().target_path().empty()) {
            md = gkfs::utils::get_metadata(md.value().target_path(), false);
            if(!md) {
                return -1;
            }
        }
    }
#endif
#endif
    struct stat tmp {};

    gkfs::utils::metadata_to_stat(path, *md, tmp);

    buf->stx_mask = 0;
    buf->stx_blksize = tmp.st_blksize;
    buf->stx_attributes = 0;
    buf->stx_nlink = tmp.st_nlink;
    buf->stx_uid = tmp.st_uid;
    buf->stx_gid = tmp.st_gid;
    buf->stx_mode = tmp.st_mode;
    buf->stx_ino = tmp.st_ino;
    buf->stx_size = tmp.st_size;
    buf->stx_blocks = tmp.st_blocks;
    buf->stx_attributes_mask = 0;

    buf->stx_atime.tv_sec = tmp.st_atim.tv_sec;
    buf->stx_atime.tv_nsec = tmp.st_atim.tv_nsec;

    buf->stx_mtime.tv_sec = tmp.st_mtim.tv_sec;
    buf->stx_mtime.tv_nsec = tmp.st_mtim.tv_nsec;

    buf->stx_ctime.tv_sec = tmp.st_ctim.tv_sec;
    buf->stx_ctime.tv_nsec = tmp.st_ctim.tv_nsec;

    buf->stx_btime = buf->stx_atime;

    return 0;
}

#endif

/**
 * gkfs wrapper for statfs() system calls
 * errno may be set
 * @param buf
 * @return 0 on success, -1 on failure
 */
int
gkfs_statfs(struct statfs* buf) {

    auto ret = gkfs::rpc::forward_get_chunk_stat();
    auto err = ret.first;
    if(err) {
        LOG(ERROR, "{}() Failure with error: '{}'", err);
        errno = err;
        return -1;
    }
    auto blk_stat = ret.second;
    buf->f_type = 0;
    buf->f_bsize = blk_stat.chunk_size;
    buf->f_blocks = blk_stat.chunk_total;
    buf->f_bfree = blk_stat.chunk_free;
    buf->f_bavail = blk_stat.chunk_free;
    buf->f_files = 0;
    buf->f_ffree = 0;
    buf->f_fsid = {0, 0};
    buf->f_namelen = path::max_length;
    buf->f_frsize = 0;
    buf->f_flags =
            ST_NOATIME | ST_NODIRATIME | ST_NOSUID | ST_NODEV | ST_SYNCHRONOUS;
    return 0;
}

#ifdef GKFS_ENABLE_UNUSED_FUNCTIONS
/**
 * gkfs wrapper for statvfs() system calls
 * errno may be set
 *
 * NOTE: Currently unused.
 *
 * @param buf
 * @return 0 on success, -1 on failure
 */
int
gkfs_statvfs(struct statvfs* buf) {
    auto ret = gkfs::rpc::forward_get_chunk_stat();
    auto err = ret.first;
    if(err) {
        LOG(ERROR, "{}() Failure with error: '{}'", err);
        errno = err;
        return -1;
    }
    auto blk_stat = ret.second;
    buf->f_bsize = blk_stat.chunk_size;
    buf->f_blocks = blk_stat.chunk_total;
    buf->f_bfree = blk_stat.chunk_free;
    buf->f_bavail = blk_stat.chunk_free;
    buf->f_files = 0;
    buf->f_ffree = 0;
    buf->f_favail = 0;
    buf->f_fsid = 0;
    buf->f_namemax = path::max_length;
    buf->f_frsize = 0;
    buf->f_flag =
            ST_NOATIME | ST_NODIRATIME | ST_NOSUID | ST_NODEV | ST_SYNCHRONOUS;
    return 0;
}
#endif

/**
 * gkfs wrapper for lseek() system calls with available file descriptor
 * errno may be set
 * @param fd
 * @param offset
 * @param whence
 * @return 0 on success, -1 on failure
 */
off_t
gkfs_lseek(unsigned int fd, off_t offset, unsigned int whence) {
    return gkfs_lseek(CTX->file_map()->get(fd), offset, whence);
}

/**
 * gkfs wrapper for lseek() system calls with available shared ptr to gkfs
 * FileMap errno may be set
 * @param gkfs_fd
 * @param offset
 * @param whence
 * @return 0 on success, -1 on failure
 */
off_t
gkfs_lseek(shared_ptr<gkfs::filemap::OpenFile> gkfs_fd, off_t offset,
           unsigned int whence) {
    switch(whence) {
        case SEEK_SET:
            if(offset < 0) {
                errno = EINVAL;
                return -1;
            }
            gkfs_fd->pos(offset);
            break;
        case SEEK_CUR:
            gkfs_fd->pos(gkfs_fd->pos() + offset);
            break;
        case SEEK_END: {
            // TODO: handle replicas
            auto ret =
                    gkfs::rpc::forward_get_metadentry_size(gkfs_fd->path(), 0);
            auto err = ret.first;
            if(err) {
                errno = err;
                return -1;
            }

            auto file_size = ret.second;
            if(offset < 0 && file_size < -offset) {
                errno = EINVAL;
                return -1;
            }
            gkfs_fd->pos(file_size + offset);
            break;
        }
        case SEEK_DATA:
            LOG(WARNING, "SEEK_DATA whence is not supported");
            // We do not support this whence yet
            errno = EINVAL;
            return -1;
        case SEEK_HOLE:
            LOG(WARNING, "SEEK_HOLE whence is not supported");
            // We do not support this whence yet
            errno = EINVAL;
            return -1;
        default:
            LOG(WARNING, "Unknown whence value {:#x}", whence);
            errno = EINVAL;
            return -1;
    }
    return gkfs_fd->pos();
}

/**
 * wrapper function for gkfs_truncate
 * errno may be set
 * @param path
 * @param old_size
 * @param new_size
 * @return 0 on success, -1 on failure
 */
int
gkfs_truncate(const std::string& path, off_t old_size, off_t new_size) {
    assert(new_size >= 0);
    assert(new_size <= old_size);

    if(new_size == old_size) {
        return 0;
    }
    for(auto copy = 0; copy < (CTX->get_replicas() + 1); copy++) {
        auto err = gkfs::rpc::forward_decr_size(path, new_size, copy);
        if(err) {
            LOG(DEBUG, "Failed to decrease size");
            errno = err;
            return -1;
        }
    }

    auto err = gkfs::rpc::forward_truncate(path, old_size, new_size,
                                           CTX->get_replicas());
    if(err) {
        LOG(DEBUG, "Failed to truncate data");
        errno = err;
        return -1;
    }
    return 0;
}

/**
 * gkfs wrapper for truncate() system calls
 * errno may be set
 * @param path
 * @param length
 * @return 0 on success, -1 on failure
 */
int
gkfs_truncate(const std::string& path, off_t length) {
    /* TODO CONCURRENCY:
     * At the moment we first ask the length to the metadata-server in order to
     * know which data-server have data to be deleted.
     *
     * From the moment we issue the gkfs_stat and the moment we issue the
     * gkfs_trunc_data, some more data could have been added to the file and the
     * length increased.
     */
    if(length < 0) {
        LOG(DEBUG, "Length is negative: {}", length);
        errno = EINVAL;
        return -1;
    }

    auto md = gkfs::utils::get_metadata(path, true);
    if(!md) {
        return -1;
    }

    // If rename is enabled we need to check if the file is renamed
#ifdef HAS_SYMLINKS
#ifdef HAS_RENAME
    if(md.value().blocks() == -1) {
        errno = ENOENT;
        return -1;
    } else if(!md.value().target_path().empty()) {
        std::string new_path;
        while(!md.value().target_path().empty()) {
            new_path = md.value().target_path();
            md = gkfs::utils::get_metadata(md.value().target_path());
        }
        // This could be optimized
        auto size = md->size();
        if(static_cast<unsigned long>(length) > size) {
            LOG(DEBUG, "Length is greater then file size: {} > {}", length,
                size);
            errno = EINVAL;
            return -1;
        }
        return gkfs_truncate(new_path, size, length);
    }
#endif
#endif

    auto size = md->size();
    if(static_cast<unsigned long>(length) > size) {
        LOG(DEBUG, "Length is greater then file size: '{}' > '{}'", length,
            size);
        auto output_fd = gkfs_open(path, md->mode(), O_WRONLY);
        if(output_fd == -1) {
            errno = EINVAL;
            return -1;
        }
        gkfs_lseek(output_fd, 0, SEEK_END);
        ssize_t n = static_cast<unsigned long>(length) - size;
        // Zeroes the buffer. All make_* are value initialized
        auto buf = std::make_unique<char[]>(n);
        if(!buf) {
            errno = ENOMEM;
            return -1;
        }
        if(gkfs_write(output_fd, buf.get(), (size_t) n) != n) {
            errno = EINVAL;
            return -1;
        }
        CTX->file_map()->remove(output_fd);
        return 0;
    }
    return gkfs_truncate(path, size, length);
}

/**
 * gkfs wrapper for dup() system calls
 * errno may be set
 * @param oldfd
 * @return file descriptor int or -1 on error
 */
int
gkfs_dup(const int oldfd) {
    return CTX->file_map()->dup(oldfd);
}

/**
 * gkfs wrapper for dup2() system calls
 * errno may be set
 * @param oldfd
 * @param newfd
 * @return file descriptor int or -1 on error
 */
int
gkfs_dup2(const int oldfd, const int newfd) {
    return CTX->file_map()->dup2(oldfd, newfd);
}

/**
 * Wrapper function for all gkfs write operations
 * errno may be set
 * @param file
 * @param buf
 * @param count
 * @param offset
 * @param update_pos pos should only be updated for some write operations (see
 * man 2 pwrite)
 * @return written size or -1 on error
 */
ssize_t
gkfs_pwrite(std::shared_ptr<gkfs::filemap::OpenFile> file, const char* buf,
            size_t count, off64_t offset, bool update_pos) {
    if(file->type() != gkfs::filemap::FileType::regular) {
        assert(file->type() == gkfs::filemap::FileType::directory);
        LOG(WARNING, "Cannot write to directory");
        errno = EISDIR;
        return -1;
    }
    auto path = make_unique<string>(file->path());
    auto is_append = file->get_flag(gkfs::filemap::OpenFile_flags::append);
    auto write_size = 0;
    auto num_replicas = CTX->get_replicas();

    auto ret_offset = gkfs::rpc::forward_update_metadentry_size(
            *path, count, offset, is_append, num_replicas);
    auto err = ret_offset.first;
    if(err) {
        LOG(ERROR, "update_metadentry_size() failed with err '{}'", err);
        errno = err;
        return -1;
    }
    if(is_append) {
        // When append is set the EOF is set to the offset
        // forward_update_metadentry_size returns. This is because it is an
        // atomic operation on the server and reserves the space for this append
        if(ret_offset.second == -1) {
            LOG(ERROR,
                "update_metadentry_size() received -1 as starting offset. "
                "This occurs when the staring offset could not be extracted "
                "from RocksDB's merge operations. Inform GekkoFS devs.");
            errno = EIO;
            return -1;
        }
        offset = ret_offset.second;
    }

    auto ret_write = gkfs::rpc::forward_write(*path, buf, offset, count, 0);
    err = ret_write.first;
    write_size = ret_write.second;

    if(num_replicas > 0) {
        auto ret_write_repl = gkfs::rpc::forward_write(*path, buf, offset,
                                                       count, num_replicas);

        if(err and ret_write_repl.first == 0) {
            // We succesfully write the data to some replica
            err = ret_write_repl.first;
            // Write size will be wrong
            write_size = ret_write_repl.second;
        }
    }

    if(err) {
        LOG(WARNING, "gkfs::rpc::forward_write() failed with err '{}'", err);
        errno = err;
        return -1;
    }
    if(update_pos) {
        // Update offset in file descriptor in the file map
        file->pos(offset + write_size);
    }
    if(static_cast<size_t>(write_size) != count) {
        LOG(WARNING,
            "gkfs::rpc::forward_write() wrote '{}' bytes instead of '{}'",
            write_size, count);
    }
    return write_size; // return written size
}

/**
 * gkfs wrapper for pwrite() system calls
 * errno may be set
 * @param fd
 * @param buf
 * @param count
 * @param offset
 * @return written size or -1 on error
 */
ssize_t
gkfs_pwrite_ws(int fd, const void* buf, size_t count, off64_t offset) {
    auto file = CTX->file_map()->get(fd);
    return gkfs_pwrite(file, reinterpret_cast<const char*>(buf), count, offset);
}

/**
 * gkfs wrapper for write() system calls
 * errno may be set
 * @param fd
 * @param buf
 * @param count
 * @return written size or -1 on error
 */
ssize_t
gkfs_write(int fd, const void* buf, size_t count) {
    auto gkfs_fd = CTX->file_map()->get(fd);
    // call pwrite and update pos
    auto ret = gkfs_pwrite(gkfs_fd, reinterpret_cast<const char*>(buf), count,
                           gkfs_fd->pos(), true);
    return ret;
}

/**
 * gkfs wrapper for pwritev() system calls
 * errno may be set
 * @param fd
 * @param iov
 * @param iovcnt
 * @param offset
 * @return written size or -1 on error
 */
ssize_t
gkfs_pwritev(int fd, const struct iovec* iov, int iovcnt, off_t offset) {

    auto file = CTX->file_map()->get(fd);
    auto pos = offset; // keep track of current position
    ssize_t written = 0;
    ssize_t ret;
    for(int i = 0; i < iovcnt; ++i) {
        auto count = (iov + i)->iov_len;
        if(count == 0) {
            continue;
        }
        auto buf = (iov + i)->iov_base;
        ret = gkfs_pwrite(file, reinterpret_cast<char*>(buf), count, pos);
        if(ret == -1) {
            break;
        }
        written += ret;
        pos += ret;

        if(static_cast<size_t>(ret) < count) {
            break;
        }
    }

    if(written == 0) {
        return -1;
    }
    return written;
}

/**
 * gkfs wrapper for writev() system calls
 * errno may be set
 * @param fd
 * @param iov
 * @param iovcnt
 * @return written size or -1 on error
 */
ssize_t
gkfs_writev(int fd, const struct iovec* iov, int iovcnt) {

    auto gkfs_fd = CTX->file_map()->get(fd);
    auto pos = gkfs_fd->pos(); // retrieve the current offset
    auto ret = gkfs_pwritev(fd, iov, iovcnt, pos);
    assert(ret != 0);
    if(ret < 0) {
        return -1;
    }
    gkfs_fd->pos(pos + ret);
    return ret;
}

/**
 * Wrapper function for all gkfs read operations
 * @param file
 * @param buf
 * @param count
 * @param offset
 * @return read size or -1 on error
 */
ssize_t
gkfs_pread(std::shared_ptr<gkfs::filemap::OpenFile> file, char* buf,
           size_t count, off64_t offset) {
    if(file->type() != gkfs::filemap::FileType::regular) {
        assert(file->type() == gkfs::filemap::FileType::directory);
        LOG(WARNING, "Cannot read from directory");
        errno = EISDIR;
        return -1;
    }

    // Zeroing buffer before read is only relevant for sparse files. Otherwise
    // sparse regions contain invalid data.
    if constexpr(gkfs::config::io::zero_buffer_before_read) {
        memset(buf, 0, sizeof(char) * count);
    }
    std::pair<int, off_t> ret;
    std::set<int8_t> failed; // set with failed targets.
    if(CTX->get_replicas() != 0) {

        ret = gkfs::rpc::forward_read(file->path(), buf, offset, count,
                                      CTX->get_replicas(), failed);
        while(ret.first == EIO) {
            ret = gkfs::rpc::forward_read(file->path(), buf, offset, count,
                                          CTX->get_replicas(), failed);
            LOG(WARNING, "gkfs::rpc::forward_read() failed with ret '{}'",
                ret.first);
        }

    } else {
        ret = gkfs::rpc::forward_read(file->path(), buf, offset, count, 0,
                                      failed);
    }

    auto err = ret.first;
    if(err) {
        LOG(WARNING, "gkfs::rpc::forward_read() failed with ret '{}'", err);
        errno = err;
        return -1;
    }
    // XXX check that we don't try to read past end of the file
    return ret.second; // return read size
}

/**
 * gkfs wrapper for read() system calls
 * errno may be set
 * @param fd
 * @param buf
 * @param count
 * @return read size or -1 on error
 */
ssize_t
gkfs_read(int fd, void* buf, size_t count) {
    auto gkfs_fd = CTX->file_map()->get(fd);
    auto pos = gkfs_fd->pos(); // retrieve the current offset
    auto ret = gkfs_pread(gkfs_fd, reinterpret_cast<char*>(buf), count, pos);
    // Update offset in file descriptor in the file map
    if(ret > 0) {
        gkfs_fd->pos(pos + ret);
    }
    return ret;
}

/**
 * gkfs wrapper for preadv() system calls
 * errno may be set
 * @param fd
 * @param iov
 * @param iovcnt
 * @param offset
 * @return read size or -1 on error
 */
ssize_t
gkfs_preadv(int fd, const struct iovec* iov, int iovcnt, off_t offset) {

    auto file = CTX->file_map()->get(fd);
    auto pos = offset; // keep track of current position
    ssize_t read = 0;
    ssize_t ret;
    for(int i = 0; i < iovcnt; ++i) {
        auto count = (iov + i)->iov_len;
        if(count == 0) {
            continue;
        }
        auto buf = (iov + i)->iov_base;
        ret = gkfs_pread(file, reinterpret_cast<char*>(buf), count, pos);
        if(ret == -1) {
            break;
        }
        read += ret;
        pos += ret;

        if(static_cast<size_t>(ret) < count) {
            break;
        }
    }

    if(read == 0) {
        return -1;
    }
    return read;
}

/**
 * gkfs wrapper for readv() system calls
 * errno may be set
 * @param fd
 * @param iov
 * @param iovcnt
 * @return read size or -1 on error
 */
ssize_t
gkfs_readv(int fd, const struct iovec* iov, int iovcnt) {

    auto gkfs_fd = CTX->file_map()->get(fd);
    auto pos = gkfs_fd->pos(); // retrieve the current offset
    auto ret = gkfs_preadv(fd, iov, iovcnt, pos);
    assert(ret != 0);
    if(ret < 0) {
        return -1;
    }
    gkfs_fd->pos(pos + ret);
    return ret;
}

/**
 * gkfs wrapper for pread() system calls
 * errno may be set
 * @param fd
 * @param buf
 * @param count
 * @param offset
 * @return read size or -1 on error
 */
ssize_t
gkfs_pread_ws(int fd, void* buf, size_t count, off64_t offset) {
    auto gkfs_fd = CTX->file_map()->get(fd);
    return gkfs_pread(gkfs_fd, reinterpret_cast<char*>(buf), count, offset);
}

/**
 * wrapper function for opening directories
 * errno may be set
 * @param path
 * @return 0 on success or -1 on error
 */
int
gkfs_opendir(const std::string& path) {
    auto md = gkfs::utils::get_metadata(path);
    if(!md) {
        return -1;
    }

    if(!S_ISDIR(md->mode())) {
        LOG(DEBUG, "Path is not a directory");
        errno = ENOTDIR;
        return -1;
    }

    auto ret = gkfs::rpc::forward_get_dirents(path);
    auto err = ret.first;
    if(err) {
        errno = err;
        return -1;
    }
    assert(ret.second);
    return CTX->file_map()->add(ret.second);
}

/**
 * gkfs wrapper for rmdir() system calls
 * errno may be set
 * @param path
 * @return 0 on success or -1 on error
 */
int
gkfs_rmdir(const std::string& path) {
    auto md = gkfs::utils::get_metadata(path);
    if(!md) {
        LOG(DEBUG, "Error: Path '{}' err code '{}' ", path, strerror(errno));
        return -1;
    }
    if(!S_ISDIR(md->mode())) {
        LOG(DEBUG, "Path '{}' is not a directory", path);
        errno = ENOTDIR;
        return -1;
    }

    auto ret = gkfs::rpc::forward_get_dirents(path);
    auto err = ret.first;
    if(err) {
        errno = err;
        return -1;
    }
    assert(ret.second);
    auto open_dir = ret.second;
    if(open_dir->size() != 0) {
        errno = ENOTEMPTY;
        return -1;
    }
    err = gkfs::rpc::forward_remove(path, CTX->get_replicas());
    if(err) {
        errno = err;
        return -1;
    }
    return 0;
}

/**
 * gkfs wrapper for getdents() system calls
 * errno may be set
 * @param fd
 * @param dirp
 * @param count
 * @return 0 on success or -1 on error
 */
int
gkfs_getdents(unsigned int fd, struct linux_dirent* dirp, unsigned int count) {

    // Get opendir object (content was downloaded with opendir() call)
    auto open_dir = CTX->file_map()->get_dir(fd);
    if(open_dir == nullptr) {
        // Cast did not succeeded: open_file is a regular file
        errno = EBADF;
        return -1;
    }

    // get directory position of which entries to return
    auto pos = open_dir->pos();
    if(pos >= open_dir->size()) {
        return 0;
    }

    unsigned int written = 0;
    struct linux_dirent* current_dirp = nullptr;
    while(pos < open_dir->size()) {
        // get dentry fir current position
        auto de = open_dir->getdent(pos);
        /*
         * Calculate the total dentry size within the kernel struct
         * `linux_dirent` depending on the file name size. The size is then
         * aligned to the size of `long` boundary. This line was originally
         * defined in the linux kernel: fs/readdir.c in function filldir(): int
         * reclen = ALIGN(offsetof(struct linux_dirent, d_name) + namlen + 2,
         * sizeof(long)); However, since d_name is null-terminated and
         * de.name().size() does not include space for the null-terminator, we
         * add 1. Thus, + 3 in total.
         */
        auto total_size = ALIGN(offsetof(struct linux_dirent, d_name) +
                                        de.name().size() + 3,
                                sizeof(long));
        if(total_size > (count - written)) {
            // no enough space left on user buffer to insert next dirent
            break;
        }
        current_dirp = reinterpret_cast<struct linux_dirent*>(
                reinterpret_cast<char*>(dirp) + written);
        current_dirp->d_ino =
                std::hash<std::string>()(open_dir->path() + "/" + de.name());

        current_dirp->d_reclen = total_size;

        *(reinterpret_cast<char*>(current_dirp) + total_size - 1) =
                ((de.type() == gkfs::filemap::FileType::regular) ? DT_REG
                                                                 : DT_DIR);

        LOG(DEBUG, "name {}: {}", pos, de.name());
        std::strcpy(&(current_dirp->d_name[0]), de.name().c_str());
        ++pos;
        current_dirp->d_off = pos;
        written += total_size;
    }

    if(written == 0) {
        errno = EINVAL;
        return -1;
    }
    // set directory position for next getdents() call
    open_dir->pos(pos);
    return written;
}

/**
 * gkfs wrapper for getdents64() system calls
 * errno may be set
 * @param fd
 * @param dirp
 * @param count
 * @return 0 on success or -1 on error
 */
int
gkfs_getdents64(unsigned int fd, struct linux_dirent64* dirp,
                unsigned int count) {

    auto open_dir = CTX->file_map()->get_dir(fd);
    if(open_dir == nullptr) {
        // Cast did not succeeded: open_file is a regular file
        errno = EBADF;
        return -1;
    }
    auto pos = open_dir->pos();
    if(pos >= open_dir->size()) {
        return 0;
    }
    unsigned int written = 0;
    struct linux_dirent64* current_dirp = nullptr;
    while(pos < open_dir->size()) {
        auto de = open_dir->getdent(pos);
        /*
         * Calculate the total dentry size within the kernel struct
         * `linux_dirent` depending on the file name size. The size is then
         * aligned to the size of `long` boundary.
         *
         * This line was originally defined in the linux kernel: fs/readdir.c in
         * function filldir64(): int reclen = ALIGN(offsetof(struct
         * linux_dirent64, d_name) + namlen + 1, sizeof(u64)); We keep + 1
         * because: Since d_name is null-terminated and de.name().size() does
         * not include space for the null-terminator, we add 1. Since d_name in
         * our `struct linux_dirent64` definition is not a zero-size array (as
         * opposed to the kernel version), we subtract 1. Thus, it stays + 1.
         */
        auto total_size = ALIGN(offsetof(struct linux_dirent64, d_name) +
                                        de.name().size() + 1,
                                sizeof(uint64_t));
        if(total_size > (count - written)) {
            // no enough space left on user buffer to insert next dirent
            break;
        }
        current_dirp = reinterpret_cast<struct linux_dirent64*>(
                reinterpret_cast<char*>(dirp) + written);
        current_dirp->d_ino =
                std::hash<std::string>()(open_dir->path() + "/" + de.name());

        current_dirp->d_reclen = total_size;
        current_dirp->d_type =
                ((de.type() == gkfs::filemap::FileType::regular) ? DT_REG
                                                                 : DT_DIR);

        LOG(DEBUG, "name {}: {}", pos, de.name());
        std::strcpy(&(current_dirp->d_name[0]), de.name().c_str());
        ++pos;
        current_dirp->d_off = pos;
        written += total_size;
    }

    if(written == 0) {
        errno = EINVAL;
        return -1;
    }
    open_dir->pos(pos);
    return written;
}


#ifdef HAS_SYMLINKS
#ifdef GKFS_ENABLE_UNUSED_FUNCTIONS
/**
 * gkfs wrapper for make symlink() system calls
 * errno may be set
 *
 * * NOTE: Currently unused
 *
 * @param path
 * @param target_path
 * @return 0 on success or -1 on error
 */
int
gkfs_mk_symlink(const std::string& path, const std::string& target_path) {
    /* The following check is not POSIX compliant.
     * In POSIX the target is not checked at all.
     *  Here if the target is a directory we raise a NOTSUP error.
     *  So that application know we don't support link to directory.
     */
    auto target_md = gkfs::utils::get_metadata(target_path, false);
    if(target_md) {
        auto trg_mode = target_md->mode();
        if(!(S_ISREG(trg_mode) || S_ISLNK(trg_mode))) {
            assert(S_ISDIR(trg_mode));
            LOG(DEBUG, "Target path is a directory. Not supported");
            errno = ENOTSUP;
            return -1;
        }
    }

    if(check_parent_dir(path)) {
        return -1;
    }

    auto link_md = gkfs::utils::get_metadata(path, false);
    if(link_md) {
        LOG(DEBUG, "Link exists: '{}'", path);
        errno = EEXIST;
        return -1;
    }

    auto err = gkfs::rpc::forward_mk_symlink(path, target_path);
    if(err) {
        errno = err;
        return -1;
    }
    return 0;
}

/**
 * gkfs wrapper for reading symlinks
 * errno may be set
 *
 * NOTE: Currently unused
 *
 * @param path
 * @param buf
 * @param bufsize
 * @return 0 on success or -1 on error
 */
int
gkfs_readlink(const std::string& path, char* buf, int bufsize) {
    auto md = gkfs::utils::get_metadata(path, false);
    if(!md) {
        LOG(DEBUG, "Named link doesn't exist");
        return -1;
    }
    if(!(md->is_link())) {
        LOG(DEBUG, "The named file is not a symbolic link");
        errno = EINVAL;
        return -1;
    }
    int path_size = md->target_path().size() + CTX->mountdir().size();
    if(path_size >= bufsize) {
        LOG(WARNING, "Destination buffer size is too short: {} < {}, {} ",
            bufsize, path_size, md->target_path());
        errno = ENAMETOOLONG;
        return -1;
    }

    CTX->mountdir().copy(buf, CTX->mountdir().size());
    std::strcpy(buf + CTX->mountdir().size(), md->target_path().c_str());
    return path_size;
}
#endif
#endif

} // namespace gkfs::syscall


/* This function defines an extension of the dirents prepared to do a find-like
 * function The function only sends the request to the specified server
 */
extern "C" int
gkfs_getsingleserverdir(const char* path, struct dirent_extended* dirp,
                        unsigned int count, int server) {

    auto ret = gkfs::rpc::forward_get_dirents_single(path, server);
    auto err = ret.first;
    if(err) {
        errno = err;
        return -1;
    }

    auto open_dir = ret.second;
    unsigned int pos = 0;
    unsigned int written = 0;
    struct dirent_extended* current_dirp = nullptr;
    while(pos < open_dir.size()) {
        auto de = open_dir[pos];
        /*
         * Calculate the total dentry size within the 'dirent_extended`
         * depending on the file name size. The size is then aligned to the size
         * of `long` boundary.
         */
        auto total_size = ALIGN(offsetof(struct dirent_extended, d_name) +
                                        (get<0>(de)).size() + 1,
                                sizeof(uint64_t));
        if(total_size > (count - written)) {
            // no enough space left on user buffer to insert next dirent
            break;
        }
        current_dirp = reinterpret_cast<struct dirent_extended*>(
                reinterpret_cast<char*>(dirp) + written);

        current_dirp->d_reclen = total_size;
        current_dirp->d_type = get<1>(de);
        current_dirp->size = get<2>(de);
        current_dirp->ctime = get<3>(de);

        LOG(DEBUG, "name {}: {} {} {} {} / size {}", pos, get<0>(de),
            get<1>(de), get<2>(de), get<3>(de), total_size);
        std::strcpy(&(current_dirp->d_name[0]), (get<0>(de)).c_str());
        ++pos;
        written += total_size;
    }

    if(written == 0) {
        errno = EINVAL;
        return -1;
    }
    return written;
}
