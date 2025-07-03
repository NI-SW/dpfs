#include <linux/module.h>
#include <linux/fs.h>
#include <linux/signal.h>
#include <linux/fs_context.h>
#include <linux/mount.h>
#include <linux/kernel.h>
#include <linux/magic.h>

#define B_SIZE 4096 // Block size for FoodFS

unsigned char block[B_SIZE];
// inode operations
int foodfs_create (struct mnt_idmap *mntid, struct inode *dir, struct dentry *dentry, umode_t mode, bool excl);
int foodfs_rmdir (struct inode *inode, struct dentry* dentry);
int foodfs_mkdir (struct mnt_idmap *mntid, struct inode*, struct dentry*, umode_t);

// file operations
ssize_t foodfs_write (struct file *file, const char __user *buf, size_t count, loff_t *pos);
ssize_t foodfs_read(struct file *file, char __user *buf, size_t count, loff_t *pos);
int foodfs_open(struct inode *inode, struct file *file);
int foodfs_release (struct inode *inode, struct file *file);

// superblock operations
struct inode *foodfs_alloc_inode(struct super_block *sb);
void foodfs_destroy_inode(struct inode *inode);

// filesystem operations
int food_init_fs_context(struct fs_context *fc);
struct dentry* food_mount(struct file_system_type *fs_type, int flags, const char *dev_name, void *data);
void food_kill_sb(struct super_block* sb);
int foodfs_fill_super(struct super_block *s, void *data, int silent);


struct foodfs_inode_info {
	struct inode		i_vnode;

	/* File zone type */
	// enum zonefs_ztype	i_ztype;

	/* File zone start sector (512B unit) */
	sector_t		i_zsector;

	/* File zone write pointer position (sequential zones only) */
	loff_t			i_wpoffset;

	/* File maximum size */
	loff_t			i_max_size;

	/* File zone size */
	loff_t			i_zone_size;

	struct mutex		i_truncate_mutex;
	struct rw_semaphore	i_mmap_sem;

	/* guarded by i_truncate_mutex */
	unsigned int		i_wr_refcnt;
	unsigned int		i_flags;
};

static struct kmem_cache *foodfs_inode_cachep;



static struct file_operations foodfs_file_ops = {
    // Define file operations here
    .write = foodfs_write,
    .read = foodfs_read,
    .open = foodfs_open,
    .release = foodfs_release,

};
static struct inode_operations foodfs_inode_ops = {
    .create = foodfs_create,
    .mkdir = foodfs_mkdir,
    .rmdir = foodfs_rmdir
};

static struct file_system_type foodfs = {
    .owner = THIS_MODULE,
    .name = "foodfs",
    // .init_fs_context = food_init_fs_context,
    .mount = food_mount,
    .kill_sb = food_kill_sb,
    .fs_flags = FS_USERNS_MOUNT, // | FS_DISALLOW_NOTIFY_PERM,
};


static struct super_operations foodfs_sops = {
	.statfs		= simple_statfs,
    .alloc_inode = foodfs_alloc_inode,
    
};
