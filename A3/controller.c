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
#include <signal.h>
#include <sys/wait.h>
#include "raid.h"

/*
 * This file implements the RAID controller that manages communication between
 * the main RAID simulator and the individual disk processes. It uses pipes
 * for inter-process communication (IPC) and fork to create child processes
 * for each disk.
 */

// Global array to store information about each disk's communication pipes.
static disk_controller_t* controllers;

/* Ignoring SIGPIPE allows us to check write calls for error rather than
 * terminating the whole system.
 */
static void ignore_sigpipe() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));

    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) == -1) {
        perror("Failed to set up SIGPIPE handler");
    }
}

/* Initialize the num-th disk controller, creating pipes to communicate
 * and creating a child process to handle disk requests.
 *
 * Returns 0 on success and -1 on failure.
 */
static int init_disk(int num) {
    ignore_sigpipe();

    // Create pipes for communication with the disk process
    if (pipe(controllers[num].to_disk) == -1 || pipe(controllers[num].from_disk) == -1) {
        perror("pipe");
        return -1;
    }

    // Fork a new process fxor the disk
    controllers[num].pid = fork();
    if (controllers[num].pid < 0) {
        perror("fork");
        return -1;
    }
    // if in the child process close the unused ends of the pipes
    if (controllers[num].pid == 0) {
        // Child process: close the unused ends of the pipes
        close(controllers[num].to_disk[1]);
        close(controllers[num].from_disk[0]);

        // Start the disk process
        if (start_disk(num, controllers[num].from_disk[1], controllers[num].to_disk[0]) != 0) {
            fprintf(stderr, "Start Disk process failed\n");
            return -1;
        }
    } 
    else {
        // Parent process: close the unused ends of the pipes
        close(controllers[num].to_disk[0]);
        close(controllers[num].from_disk[1]);
    }
    return 0;
}

/* Restart the num-th disk, whose process is assumed to have already been killed.
 *
 * This function is very similar to init_disk.
 * However, since the other processes have all been started,
 * it needs to close a larger set of open pipe descriptors
 * inherited from the parent.
 *
 * Returns 0 on success and -1 on failure.
*/
int restart_disk(int num) {
    ignore_sigpipe();

    // Create pipes for communication with the disk process
    if (pipe(controllers[num].to_disk) == -1 || pipe(controllers[num].from_disk) == -1) {
        perror("pipe");
        return -1;
    }

    // Fork a new process for the disk
    controllers[num].pid = fork();
    if (controllers[num].pid < 0) {
        perror("fork");
        return -1;
    }
    if (controllers[num].pid == 0) {
        // for the child process close the unused ends of the pipes
        for (int i = 0; i < num_disks + 1; i++) {
            // if this is the disk we are starting at close the other ends of the pipes
            if (i != num) {
                close(controllers[i].to_disk[1]);
                close(controllers[i].from_disk[0]);
            }
            // else close the ends of the pipes that are not used by this disk
            else {
                close(controllers[i].to_disk[0]);
                close(controllers[i].from_disk[1]);
                close(controllers[i].from_disk[0]);
                close(controllers[i].to_disk[1]);
            }
        }

        // Start the disk process
        if (start_disk(num, controllers[num].from_disk[1], controllers[num].to_disk[0]) != 0) {
            fprintf(stderr, "Start Disk process failed\n");
            return -1;
        }
    } 
    else {
        // Parent process: close the unused ends of the pipes
        close(controllers[num].to_disk[0]);
        close(controllers[num].from_disk[1]);
    }
    return 0;
}

/* Initialize all disk controllers by initializing the controllers
 * array and calling init_disk for each disk.
 *
 * total_disks is the number of data disks + 1 for the parity disk.
 *
 * Returns 0 on success and -1 on failure.
 */
int init_all_controllers(int total_disks) {
    // Allocate memory for the disk controllers
    controllers = malloc(total_disks * sizeof(disk_controller_t));

    // sanity check
    if (controllers == NULL) {
        perror("malloc");
        return -1;
    }

    // Initialize the disk for each controller
    for (int i = 0; i < total_disks; i++) {
        if (init_disk(i) == -1) {
            fprintf(stderr, "Init Disk failed %d\n", i);
            free(controllers);
            return -1;
        }
    }
    return 0;
}

/* Read the block of data at block_num from the appropriate disk.
 * The block is stored to the memory pointed to by data.
 *
 * If parity_flag == 1, read from parity disk.
 * If parity_flag == 0, read from data disk.
 *
 * Returns 0 on success and -1 on failure.
 */
int read_block_from_disk(int block_num, char* data, int parity_flag) {
    if (!data) {
        fprintf(stderr, "Error: Invalid data buffer\n");
        return -1;
    }

    // Identify the stripe to read from
    int disk_num;
    if (parity_flag == 1) {
        disk_num = num_disks;
    } else {
        disk_num = block_num % num_disks;
    }

    disk_command_t cmd = CMD_READ;

    // Each disk has a linear array of blocks, so the block number on an
    // individual disk is the same as the stripe number
    block_num = block_num / num_disks;

    // Write the command and the block number to the disk process
    // Then read the block from the disk process

    // Write cmd to the disk process
    if (write(controllers[disk_num].to_disk[1], &cmd, sizeof(cmd)) != sizeof(cmd)) {
        fprintf(stderr, "read_block_from_disk: write cmd to disk failed\n");
        return -1;
    }

    // Write block_num to the disk process
    if (write(controllers[disk_num].to_disk[1], &block_num, sizeof(block_num)) != sizeof(block_num)) {
        fprintf(stderr, "read_block_from_disk: write block num to disk failed\n");
        return -1;
    }

    // Read block data from the disk process
    if (read(controllers[disk_num].from_disk[0], data, block_size) != block_size) {
        fprintf(stderr, "read_block_from_disk: read data from disk failed\n");
        return -1;
    }
    return 0;
}

/* Write a block of data to the block at block_num on the appropriate disk.
 * The block is stored at the memory pointed to by data.
 *
 * If parity_flag == 1, write to parity disk.
 * If parity_flag == 0, write to data disk.
 *
 * Returns 0 on success and -1 on failure.
 */
int write_block_to_disk(int block_num, char *data, int parity_flag) {
    if (data == NULL) {
        fprintf(stderr, "Invalid data buffer\n");
        return -1;
    }

    // Identify the stripe to read from
    int disk_num;
    if (parity_flag == 1) {
        disk_num = num_disks;
    } else {
        disk_num = block_num % num_disks;
    }

    disk_command_t cmd = CMD_WRITE;

    // Each disk has a linear array of blocks, so the block number on an
    // individual disk is the same as the stripe number
    block_num = block_num / num_disks;

    // Write the command and the block number to the disk process
    // Then read the block from the disk process

    // Write cmd to the disk process
    if (write(controllers[disk_num].to_disk[1], &cmd, sizeof(cmd)) != sizeof(cmd)) {
        fprintf(stderr, "write_block_to_disk: write cmd to disk failed\n");
        return -1;
    }

    // Write block_num to the disk process
    if (write(controllers[disk_num].to_disk[1], &block_num, sizeof(block_num)) != sizeof(block_num)) {
        fprintf(stderr, "write_block_to_disk: write block num to disk failed\n");
        return -1;
    }
    
    // Write block data to the disk process
    if (write(controllers[disk_num].to_disk[1], data, block_size) != block_size) {
        fprintf(stderr, "write_block_to_disk: write data to disk failed\n");
        return -1;
    }
    return 0;
}

/* Write the memory pointed to by data to the block at block_num on the
 * RAID system, handling parity updates.
 * If block_num is invalid (outside the range 0 to disk_size/block_size)
 * then return -1.
 *
 * Returns 0 on success and the disk number we tried to read from on failure.
 */
int write_block(int block_num, char *data) {
    if (data == NULL) {
        fprintf(stderr, "Invalid data buffer\n");
        return -1;
    }

    // Check if block_num is valid
    if (block_num < 0 || block_num >= disk_size / block_size) {
        fprintf(stderr, "Invalid block number\n");
        return -1;
    }

    // Identify the disk_num and stripe to write to
    int disk_num = block_num % num_disks;
    int stripe = block_num / num_disks;

    // Check if we are writing to the parity disk
    int parity_flag = 0;
    if (disk_num == num_disks) {
        parity_flag = 1;
    }

    // Write block data to the correct disk
    if (write_block_to_disk(block_num, data, parity_flag) != 0) {
        fprintf(stderr, "Failed to write block to disk\n");
        return -1;
    }

    // Update parity disk if necessary
    if (parity_flag == 0) {
        char *parity_data = calloc(1, block_size);
        // sanity check
        if (parity_data == NULL) {
            perror("calloc");
            return -1;
        }

        // Read data from the disk to update parity
        for (int i = 0; i < num_disks; i++) {
            if (i != disk_num) {
                char *temp_data = malloc(block_size);
                // sanity check
                if (temp_data == NULL) {
                    perror("malloc");
                    free(parity_data);
                    return -1;
                }
                if (read_block_from_disk(i, temp_data, 0) != 0) {
                    fprintf(stderr, "Failed to read block from disk\n");
                    free(temp_data);
                    free(parity_data);
                    return -1;
                }
                for (int j = 0; j < block_size; j++) {
                    parity_data[j] ^= temp_data[j];
                }
                free(temp_data);
            }
        }
        
        // Update the parity data with new block
        for (int i = 0; i < block_size; i++) {
            parity_data[i] ^= data[i];
        }

        // Write updated parity data to the parity disk
        if (write_block_to_disk(stripe, parity_data, 1) != 0) {
            fprintf(stderr, "Failed to write updated parity block\n");
            free(parity_data);
            return -1;
        }
        free(parity_data);
    }
    return 0;
}

/* Read the block at block_num from the RAID system into
 * the memory pointed to by data.
 * If block_num is invalid (outside the range 0 to disk_size/block_size)
 * then return NULL.
 *
 * Returns a pointer to the data buffer on success and NULL on failure.
 */
char *read_block(int block_num, char *data) {
    if (data == NULL) {
        fprintf(stderr, "Invalid data buffer\n");
        return NULL;
    }

    // Check if block_num is valid
    if (block_num < 0 || block_num >= disk_size / block_size) {
        fprintf(stderr, "Invalid block number\n");
        return NULL;
    }

    // Read block data from the correct disk
    if (read_block_from_disk(block_num, data, 0) != 0) {
        fprintf(stderr, "Failed to read block from disk\n");
        return NULL;
    }
    return data;
}

/* Send exit command to all disk processes.
 *
 * Returns when all disk processes have terminated.
 */
void checkpoint_and_wait() {
    for (int i = 0; i < num_disks + 1; i++) {
        disk_command_t cmd = CMD_EXIT;
        size_t bytes_written = write(controllers[i].to_disk[1], &cmd, sizeof(cmd));
        if (bytes_written != sizeof(cmd)) {
            fprintf(stderr, "Warning: Failed to send exit command to disk %d\n", i);
        }
    }
    // wait for all disks to exit
    // we aren't going to do anything with the exit value
    for (int i = 0; i < num_disks + 1; i++) {
        wait(NULL);
    }
}


/* Simulate the failure of a disk by sending the SIGINT signal to the
 * process with id disk_num.
 */
void simulate_disk_failure(int disk_num) {
    if(debug) {
        printf("Simulate: killing disk %d\n", disk_num);
    }
    kill(controllers[disk_num].pid, SIGINT);
    if (waitpid(controllers[disk_num].pid, NULL, 0) == -1) {
        perror("simulate_disk_failure: waitpid");
    }
}

/* Restore the disk process after it has been killed.
 * If some aspect of restoring the disk process fails, 
 * then you can consider it a catastropic failure and 
 * exit the program.
 */
void restore_disk_process(int disk_num) {
    if (restart_disk(disk_num) == -1) {
        fprintf(stderr, "Failed to restore disk process for disk num: %d\n", disk_num);
        exit(1);
    }
}
