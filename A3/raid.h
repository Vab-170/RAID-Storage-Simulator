/* This code is provided solely for the personal and private use of students
 * taking the CSC209H course at the University of Toronto. Copying for purposes
 * other than this use is expressly prohibited. All forms of distribution of
 * this code, including but not limited to public repositories on GitHub,
 * GitLab, Bitbucket, or any other online platform, whether as given or with
 * any changes, are expressly prohibited.
 *
 * Authors: Karen Reid, Paul He, Philip Kukulak
 *
 * All of the files in this directory and all subdirectories are:
 * Copyright (c) 2025 Karen Reid
 */
#ifndef RAID_H
#define RAID_H

#define DEFAULT_NUM_DISKS 3
#define DEFAULT_BLOCK_SIZE 16
#define DEFAULT_DISK_SIZE (16 * DEFAULT_BLOCK_SIZE)

#define MAX_NAME 32

// Disk controller structure
typedef struct {
    pid_t pid;
    int to_disk[2];         // Pipe for sending commands to disk
    int from_disk[2];       // Pipe for receiving responses from disk
} disk_controller_t;

// Command types for disk processes
typedef enum {
    CMD_READ,
    CMD_WRITE,
    CMD_EXIT
} disk_command_t;

// Command structure
typedef struct {
    char *cmd;
    char *arg1;
    char *arg2;
} command_t;

// These global configuration variables are defined and set in main
extern int num_disks;
extern int block_size;
extern int disk_size;

extern int debug;

// Controller Interface
int init_all_controllers(int num_disks);
int write_block(int block_num, char *data);
char *read_block(int block_num, char *data);
int restart_disk(int disk_num);
void simulate_disk_failure(int disk_num);
void restore_disk_process(int disk_num);
void checkpoint_and_wait();

// Disk Interface
int start_disk(int id, int to_parent, int from_parent);

#endif // RAID_H
