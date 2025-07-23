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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "raid.h"


int debug = 1;  // Set to 1 to enable debug output, 0 to disable

static int checkpoint_disk(char *disk_data, int id);

/*
 * Main function for the disk simulation process, which runs in a child process
 * created by the RAID controller.
 *
 * id is the disk number or index into the controllers table,
 * to_parent is the pipe descriptor for writing to the parent,
 * from_parent is the pipe descriptor for reading from the parent.
 *
 * Returns 0 on success and 1 on failure.
 */
int start_disk(int id, int to_parent, int from_parent) {
    int status = 0;

    // Allocate memory for disk data
    char *disk_data = calloc(disk_size, sizeof(char));
    // sanity check
    if (disk_data == NULL) {
        perror("calloc");
        return 1;
    }

    // Main command loop to handle requests from the parent.
    // This is an infinite loop that is only terminated when
    // an exit command is received.
    while (1) {
        disk_command_t cmd;

        // Read command from the parent
        if (read(from_parent, &cmd, sizeof(cmd)) != sizeof(cmd)) {
            fprintf(stderr, "Failed to read command from parent");
            status = 1;
            break;
        }

        // The type of command received from the parent
        // determines which action is taken next.
        switch (cmd) {
            case CMD_READ: {
                // declare block_num
                int block_num;

                // Read the block num from the parent
                if (read(from_parent, &block_num, sizeof(block_num)) != sizeof(block_num)) {
                    fprintf(stderr, "Failed to read block number from parent");
                    status = 1;
                    break;
                }

                // Assign disk data to the block_data
                char *block_data = disk_data + (block_num * block_size);

                // Write the block data to the parent process
                if (write(to_parent, block_data, block_size) != block_size) {
                    fprintf(stderr, "Failed to write data to parent");
                    status = 1;
                    break;
                }
                break;
            }

            case CMD_WRITE: {
                // declare block_num
                int block_num;

                // Read the block num from the parent
                if (read(from_parent, &block_num, sizeof(block_num)) != sizeof(block_num)) {
                    fprintf(stderr, "Failed to read block number from parent");
                    status = 1;
                    break;
                }

                // declare array to store block data
                char block_data[block_size];

                // Read the block data from the parent process
                if (read(from_parent, block_data, block_size) != block_size) {
                    fprintf(stderr, "Failed to read block data");
                    status = 1;
                    break;
                }

                // Store block data into the correct location
                memcpy(disk_data + (block_num * block_size), block_data, block_size);
                break;
            }

            case CMD_EXIT: {
                checkpoint_disk(disk_data, id);
                free(disk_data);
                exit(0);
            }
            default: {
                fprintf(stderr, "Error: Unknown command %d received\n", cmd);
                status = 1;
                break;
            }
        }
    }

    // Checkpoint and cleanup before exiting
    checkpoint_and_wait();
    if (disk_data) {
        free(disk_data);
    }
    
    exit(status);
}

/* Save the disk's data, pointed to by disk_data, to a file named id.
 *
 * Returns 0 on success, and -1 on failure.
 */
static int checkpoint_disk(char *disk_data, int id) {
    if (!disk_data) {
        fprintf(stderr, "Error: Invalid parameters for checkpoint\n");
        return -1;
    }

    // Create a file name for this disk
    char disk_name[MAX_NAME];
    if (snprintf(disk_name, sizeof(disk_name), "disk_%d.dat", id) >= (int)sizeof(disk_name)) {
        fprintf(stderr, "Error: Disk name too long for disk %d\n", id);
        return 1;
    }

    FILE *fp = fopen(disk_name, "wb");
    if (!fp) {
        perror("Failed to create checkpoint file");
        return -1;
    }

    size_t bytes_written = fwrite(disk_data, 1, disk_size, fp);
    if (bytes_written != (size_t)disk_size) {
        if (ferror(fp)) {
            fprintf(stderr, "Failed to write checkpoint data");
        } else {
            fprintf(stderr, "Error: Incomplete write during checkpoint\n");
        }
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        perror("Failed to close checkpoint file");
        return -1;
    }

    return 0;
}
