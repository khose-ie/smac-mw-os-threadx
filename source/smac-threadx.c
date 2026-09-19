/// @file    smac-threadx.c
/// @brief   SMAC OS abstraction layer implementation for ThreadX.
/// @details This source file provides the implementation of the SMAC OS abstraction layer
///          using the ThreadX RTOS as the underlying operating system. It includes functions
///          for task management, event handling, message queues, memory pools, mutexes,
///          semaphores, and timers.
/// @author  Khose-ie<khose-ie@outlook.com>
/// @date    2024-06-10

#include <assert.h>
#include <smac/configuration/smac-threadx.h>
#include <smac/middleware/smac-os.h>
#include <stdlib.h>
#include <tx_api.h>
#include <tx_block_pool.h>
#include <tx_byte_pool.h>
#include <tx_event_flags.h>
#include <tx_initialize.h>
#include <tx_mutex.h>
#include <tx_queue.h>
#include <tx_semaphore.h>
#include <tx_thread.h>
#include <tx_timer.h>

#ifndef SMAC_TX_OS_STACK_SIZE
#define SMAC_TX_OS_STACK_SIZE (1024 * 64)
#endif // SMAC_TX_OS_STACK_SIZE

/// @brief Size of the OS memory pool 1 blocks
/// @details This constant defines the size (in bytes) of each memory block
#ifndef SMAC_TX_OS_MEM_POOL_BKSZ_1
#define SMAC_TX_OS_MEM_POOL_BKSZ_1 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKSZ_1

/// @brief Count of the OS memory pool 1 blocks
/// @details This constant defines the number of memory blocks in the OS memory pool 1.
#ifndef SMAC_TX_OS_MEM_POOL_BKCT_1
#define SMAC_TX_OS_MEM_POOL_BKCT_1 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKCT_1

/// @brief Size of the OS memory pool 2 blocks
/// @details This constant defines the size (in bytes) of each memory block
#ifndef SMAC_TX_OS_MEM_POOL_BKSZ_2
#define SMAC_TX_OS_MEM_POOL_BKSZ_2 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKSZ_2

/// @brief Count of the OS memory pool 2 blocks
/// @details This constant defines the number of memory blocks in the OS memory pool 2.
#ifndef SMAC_TX_OS_MEM_POOL_BKCT_2
#define SMAC_TX_OS_MEM_POOL_BKCT_2 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKCT_2

/// @brief Size of the OS memory pool 3 blocks
/// @details This constant defines the size (in bytes) of each memory block
#ifndef SMAC_TX_OS_MEM_POOL_BKSZ_3
#define SMAC_TX_OS_MEM_POOL_BKSZ_3 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKSZ_3

/// @brief Count of the OS memory pool 3 blocks
/// @details This constant defines the number of memory blocks in the OS memory pool 3.
#ifndef SMAC_TX_OS_MEM_POOL_BKCT_3
#define SMAC_TX_OS_MEM_POOL_BKCT_3 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKCT_3

/// @brief Size of the OS memory pool 4 blocks
/// @details This constant defines the size (in bytes) of each memory block
#ifndef SMAC_TX_OS_MEM_POOL_BKSZ_4
#define SMAC_TX_OS_MEM_POOL_BKSZ_4 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKSZ_4

/// @brief Count of the OS memory pool 4 blocks
/// @details This constant defines the number of memory blocks in the OS memory pool 4.
#ifndef SMAC_TX_OS_MEM_POOL_BKCT_4
#define SMAC_TX_OS_MEM_POOL_BKCT_4 (0)
#endif // SMAC_TX_OS_MEM_POOL_BKCT_4

/// @brief OS stack definition
/// @details This variable defines the byte pool used for the OS stack.
#if !defined(SMAC_TX_OS_STACK_EX_MEM)
#define EXT1     static
#define OS_STACK (_os_stack)
#else // SMAC_TX_OS_STACK_EX_MEM
#define EXT1     extern
#define OS_STACK SMAC_TX_OS_STACK_EX_MEM
#endif // !defined(SMAC_TX_OS_STACK_EX_MEM)

/// @brief OS memory pool definition
/// @details This section defines the memory used for the OS memory pool.
#ifndef SMAC_TX_OS_MEM_POOL_EX_MEM
#define EXT2            static
#define OS_MEM_POOL_MEM (_os_mem_pool_mem)
#else // SMAC_TX_OS_MEM_POOL_EX_MEM
#define EXT2            extern
#define OS_MEM_POOL_MEM SMAC_TX_OS_MEM_POOL_EX_MEM
#endif // SMAC_TX_OS_MEM_POOL_EX_MEM

/// @brief Default time slice for tasks
/// @details This constant defines the default time slice (in ticks) for tasks.
#define OS_DEFAULT_TIME_SLICE (4)

/// @brief OS stack size
/// @details This constant defines the size (in bytes) of the OS stack.
#define OS_STACK_SIZE (SMAC_TX_OS_STACK_SIZE)

/// @brief Minimum required size of the OS stack
/// @details This constant defines the minimum required size (in bytes) of the OS stack, which is
/// the aligned size of the threadxStack_t structure.
#define OS_STACK_CONTROL_BLOCK_SIZE (align32up(sizeof(threadxStack_t)))

/// @brief OS stack memory size
/// @details This constant defines the size (in bytes) of the OS stack memory available for use,
/// excluding the space required for the threadxStack_t structure.
#define OS_STACK_MEM_SIZE (OS_STACK_SIZE - OS_STACK_CONTROL_BLOCK_SIZE)

/// @brief Required size of the OS stack
/// @details This constant defines the required size (in bytes) of the OS stack control block, which
/// is the aligned size of the threadxStack_t structure.
#define OS_STACK_REQUIRED_SIZE (OS_STACK_CONTROL_BLOCK_SIZE)

/// @brief OS memory pool indices
/// @details These constants define the indices for the OS memory pools.
#define OS_MEM_POOL_1   (0)
#define OS_MEM_POOL_2   (1)
#define OS_MEM_POOL_3   (2)
#define OS_MEM_POOL_4   (3)
#define OS_MEM_POOL_NUM (4)

/// @brief Sizes of individual OS memory pools
/// @details These constants define the sizes (in bytes) of each individual OS memory pool.
#define OS_MEM_POOL_MEM1_SIZE (SMAC_TX_OS_MEM_POOL_BKSZ_1 * SMAC_TX_OS_MEM_POOL_BKCT_1)
#define OS_MEM_POOL_MEM2_SIZE (SMAC_TX_OS_MEM_POOL_BKSZ_2 * SMAC_TX_OS_MEM_POOL_BKCT_2)
#define OS_MEM_POOL_MEM3_SIZE (SMAC_TX_OS_MEM_POOL_BKSZ_3 * SMAC_TX_OS_MEM_POOL_BKCT_3)
#define OS_MEM_POOL_MEM4_SIZE (SMAC_TX_OS_MEM_POOL_BKSZ_4 * SMAC_TX_OS_MEM_POOL_BKCT_4)

/// @brief Total size of the OS memory pool
/// @details This constant defines the total size (in bytes) of the OS memory pool, calculated as
/// the sum of the sizes of all memory pool blocks.
#define OS_MEM_POOL_SIZE                                                                           \
    (OS_MEM_POOL_MEM1_SIZE + OS_MEM_POOL_MEM2_SIZE + OS_MEM_POOL_MEM3_SIZE + OS_MEM_POOL_MEM4_SIZE)

/// @brief ThreadX control block
/// @details This structure holds the state and stack for the ThreadX OS.
typedef struct
{
    TX_BYTE_POOL stack;
    uint8_t* stack_mem;
    smacOsState_t state;
} threadxControlBlock_t;

/// @brief ThreadX memory pool control block
/// @details This structure holds the memory pool and its associated memory for the ThreadX OS.
typedef struct
{
    TX_BLOCK_POOL stack;
    uint8_t* mem;
} threadxMemPoolControlBlock_t;

/// @brief ThreadX stack structure
/// @details This structure holds the control block, memory pool control blocks, and stack memory
/// for the ThreadX OS.
typedef struct
{
    threadxControlBlock_t os;
    threadxMemPoolControlBlock_t mem_pool[OS_MEM_POOL_NUM];
} threadxStack_t;

/// @brief OS stack memory
/// @details This array defines the memory used for the OS stack.
EXT1 uint8_t OS_STACK[OS_STACK_SIZE];

/// @brief OS memory pool memory
/// @details This array defines the memory used for the OS memory pool.
EXT2 uint8_t OS_MEM_POOL_MEM[OS_MEM_POOL_SIZE];

/// @brief Get the instance of the ThreadX stack
/// @return Pointer to the ThreadX stack instance
#define os_instance() ((threadxStack_t*)OS_STACK)

/// @brief Allocate memory from the OS stack byte pool
/// @details This function allocates a block of memory of the specified size
///          from the OS stack byte pool.
/// @param size Size of the memory block to allocate in bytes
/// @return Pointer to the allocated memory block, or NULL on failure
static uint8_t* _mem_alloc(uint32_t size)
{
    uint8_t* mem        = NULL;
    uint32_t alloc_size = size;

    if (size == 0)
    {
        return NULL;
    }

    if (size < TX_BYTE_BLOCK_MIN)
    {
        alloc_size = TX_BYTE_BLOCK_MIN;
    }

    if (tx_byte_allocate(&os_instance()->os.stack, (VOID**)&mem, alloc_size, TX_NO_WAIT) !=
        TX_SUCCESS)
    {
        return NULL;
    }

    return mem;
}

/// @brief Free memory back to the OS stack byte pool
/// @details This function frees a previously allocated block of memory
///          back to the OS stack byte pool.
/// @param mem Pointer to the memory block to free
static void _mem_free(uint8_t* mem)
{
    if (mem != NULL)
    {
        tx_byte_release((VOID*)mem);
    }
}

/// @brief Convert ThreadX task state to SMAC task state
/// @param threadx_state ThreadX task state
/// @return Corresponding SMAC task state
static smacTaskState_t _convert_threadx_task_state(UINT threadx_state)
{
    smacTaskState_t converted_state;

    switch (threadx_state)
    {
        case TX_READY:
            converted_state = SMAC_TASK_STATE_READY;
            break;

        case TX_COMPLETED:
        case TX_TERMINATED:
            converted_state = SMAC_TASK_STATE_TERMINATED;
            break;

        case TX_SUSPENDED:
        case TX_SLEEP:
        case TX_QUEUE_SUSP:
        case TX_SEMAPHORE_SUSP:
        case TX_EVENT_FLAG:
        case TX_BLOCK_MEMORY:
        case TX_BYTE_MEMORY:
        case TX_IO_DRIVER:
        case TX_FILE:
        case TX_TCP_IP:
        case TX_MUTEX_SUSP:
        case TX_PRIORITY_CHANGE:
            converted_state = SMAC_TASK_STATE_BLOCKED;
            break;

        default:
            return SMAC_TASK_STATE_UNKNOWN;
    }

    return converted_state;
}

/// @brief Convert ThreadX task priority to SMAC task priority
/// @param threadx_priority ThreadX task priority
/// @return Corresponding SMAC task priority
static smacTaskPriority_t _convert_threadx_task_priority(UINT threadx_priority)
{
    if (threadx_priority > SMAC_TASK_PRIORITY_MAX)
    {
        return SMAC_TASK_PRIORITY_NONE;
    }

    return (smacTaskPriority_t)(SMAC_TASK_PRIORITY_MAX - threadx_priority);
}

/// @brief Initialize the OS abstraction layer
/// @details This function initializes the OS abstraction layer by creating
///          the OS stack byte pool and setting the initial OS state.
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_os_initialize(void)
{
    assert(OS_STACK_SIZE >= OS_STACK_REQUIRED_SIZE);

    memset(OS_STACK, 0, OS_STACK_SIZE);
    os_instance()->os.stack_mem = (uint8_t*)OS_STACK + OS_STACK_CONTROL_BLOCK_SIZE;

    _tx_initialize_kernel_setup();

    if (tx_byte_pool_create(&os_instance()->os.stack, "os-stack", os_instance()->os.stack_mem,
                            OS_STACK_MEM_SIZE) != TX_SUCCESS)
    {
        os_instance()->os.state = SMAC_OS_STATE_FAULT;
        return SMAC_RET_OS_MEM_POOL_ERR;
    }

    os_instance()->os.state = SMAC_OS_STATE_READY;
    return SMAC_RET_OK;
}

/// @brief Initialize the OS memory pool
/// @details This function initializes the OS memory pool used for dynamic
///          memory allocations within the OS abstraction layer.
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_os_initialize_mem_pool(void)
{
    threadxMemPoolControlBlock_t* mem_pool = NULL;

    memset(OS_MEM_POOL_MEM, 0, OS_MEM_POOL_SIZE);

#if OS_MEM_POOL_MEM1_SIZE != 0

    mem_pool      = &os_instance()->mem_pool[OS_MEM_POOL_1];
    mem_pool->mem = OS_MEM_POOL_MEM;

    if (smac_mem_pool_create_static("os-mempool-1", &mem_pool->stack, sizeof(mem_pool->stack),
                                    mem_pool->mem, SMAC_TX_OS_MEM_POOL_BKSZ_1,
                                    SMAC_TX_OS_MEM_POOL_BKCT_1) == NULL)
    {
        return SMAC_RET_OS_MEM_POOL_ERR;
    }

#endif // OS_MEM_POOL_MEM1_SIZE != 0

#if OS_MEM_POOL_MEM2_SIZE != 0

    mem_pool      = &os_instance()->mem_pool[OS_MEM_POOL_2];
    mem_pool->mem = OS_MEM_POOL_MEM + OS_MEM_POOL_MEM1_SIZE;

    if (smac_mem_pool_create_static("os-mempool-2", &mem_pool->stack, sizeof(mem_pool->stack),
                                    mem_pool->mem, SMAC_TX_OS_MEM_POOL_BKSZ_2,
                                    SMAC_TX_OS_MEM_POOL_BKCT_2) == NULL)
    {
        return SMAC_RET_OS_MEM_POOL_ERR;
    }

#endif // OS_MEM_POOL_MEM2_SIZE != 0

#if OS_MEM_POOL_MEM3_SIZE != 0

    mem_pool      = &os_instance()->mem_pool[OS_MEM_POOL_3];
    mem_pool->mem = OS_MEM_POOL_MEM + OS_MEM_POOL_MEM1_SIZE + OS_MEM_POOL_MEM2_SIZE;

    if (smac_mem_pool_create_static("os-mempool-3", &mem_pool->stack, sizeof(mem_pool->stack),
                                    mem_pool->mem, SMAC_TX_OS_MEM_POOL_BKSZ_3,
                                    SMAC_TX_OS_MEM_POOL_BKCT_3) == NULL)
    {
        return SMAC_RET_OS_MEM_POOL_ERR;
    }

#endif // OS_MEM_POOL_MEM3_SIZE != 0

#if OS_MEM_POOL_MEM4_SIZE != 0

    mem_pool = &os_instance()->mem_pool[OS_MEM_POOL_4];
    mem_pool->mem =
        OS_MEM_POOL_MEM + OS_MEM_POOL_MEM1_SIZE + OS_MEM_POOL_MEM2_SIZE + OS_MEM_POOL_MEM3_SIZE;

    if (smac_mem_pool_create_static("os-mempool-4", &mem_pool->stack, sizeof(mem_pool->stack),
                                    mem_pool->mem, SMAC_TX_OS_MEM_POOL_BKSZ_4,
                                    SMAC_TX_OS_MEM_POOL_BKCT_4) == NULL)
    {
        return SMAC_RET_OS_MEM_POOL_ERR;
    }

#endif // OS_MEM_POOL_MEM4_SIZE != 0

    return SMAC_RET_OK;
}

/// @brief Start the OS kernel
/// @details This function starts the OS kernel if it is in the initialized state.
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_os_start(void)
{
    if (os_instance()->os.state != SMAC_OS_STATE_READY)
    {
        return SMAC_RET_OS_KERNEL_ERR;
    }

    os_instance()->os.state = SMAC_OS_STATE_RUNNING;
    tx_kernel_enter();

    return SMAC_RET_OK;
}

/// @brief  Get the current OS state
/// @details This function retrieves the current state of the OS.
/// @return Current OS state as a value of type @ref smacOsState_t
smacOsState_t smac_os_state(void)
{
    return os_instance()->os.state;
}

/// @brief Get the current OS tick count
/// @return Current OS tick count
uint32_t smac_os_tick_state(void)
{
    return (uint32_t)tx_time_get();
}

/// @brief Get the current number of tasks
/// @return Current number of tasks
uint32_t smac_os_task_count(void)
{
    return (uint32_t)_tx_thread_created_count;
}

/// @brief Get the handle of the current task
/// @return Handle of the current task
smacTaskHandle_t smac_os_current_task(void)
{
    return (smacTaskHandle_t)tx_thread_identify();
}

/// @brief Delay the current task for a specified number of ticks
/// @details This function puts the current task into a blocked state for
///          the specified number of system ticks.
/// @param ticks Number of system ticks to delay
void smac_os_delay(uint32_t ticks)
{
    if (ticks != 0)
    {
        tx_thread_sleep((ULONG)ticks);
    }
}

/// @brief Delay the current task until a specified time increment has passed
/// @details This function delays the current task until the specified time
///          increment has passed since the previous wake time.
/// @param ticks     Time increment in system ticks
void smac_os_delay_interval(uint32_t ticks)
{
    smac_os_delay(ticks);
}

/// @brief Yield the processor to another ready task
/// @details This function allows the current task to yield the processor,
///          allowing other ready tasks to run.
void smac_os_switch_task(void)
{
    tx_thread_relinquish();
}

/// @brief  Exit the current task
/// @details This function terminates the execution of the current task.
void smac_os_exit_task(void)
{
    smac_task_delete(smac_os_current_task());
    while (1);
}

/// @brief  Exit the current task created with static stack allocation
/// @details This function terminates the execution of the current task created with static stack
void smac_os_exit_task_static(void)
{
    smac_task_delete_static(smac_os_current_task());
}

/// @brief  Allocate memory from the OS memory pool
/// @details This function allocates a block of memory of the specified size from the OS memory
///          pool.
/// @note    The OS implementation will choose the best fit memory block from the pool.
/// @param size Size of memory want to allocate in bytes
/// @return Pointer to the allocated memory block, or NULL on failure
void* smac_os_malloc(uint32_t size)
{
    void* mem = NULL;

    if ((os_instance()->mem_pool[OS_MEM_POOL_1].mem != NULL) &&
        (size <= SMAC_TX_OS_MEM_POOL_BKSZ_1))
    {
        mem = smac_mem_pool_alloc(&os_instance()->mem_pool[OS_MEM_POOL_1].stack, size);
    }
    else if ((os_instance()->mem_pool[OS_MEM_POOL_2].mem != NULL) &&
             (size <= SMAC_TX_OS_MEM_POOL_BKSZ_2))
    {
        mem = smac_mem_pool_alloc(&os_instance()->mem_pool[OS_MEM_POOL_2].stack, size);
    }
    else if ((os_instance()->mem_pool[OS_MEM_POOL_3].mem != NULL) &&
             (size <= SMAC_TX_OS_MEM_POOL_BKSZ_3))
    {
        mem = smac_mem_pool_alloc(&os_instance()->mem_pool[OS_MEM_POOL_3].stack, size);
    }
    else if ((os_instance()->mem_pool[OS_MEM_POOL_4].mem != NULL) &&
             (size <= SMAC_TX_OS_MEM_POOL_BKSZ_4))
    {
        mem = smac_mem_pool_alloc(&os_instance()->mem_pool[OS_MEM_POOL_4].stack, size);
    }

    return mem;
}

/// @brief  Free memory back to the OS memory pool
/// @details This function frees a previously allocated block of memory back to the OS memory pool.
/// @param mem Pointer to the memory block to free
/// @param size Size of the memory to free in bytes
void smac_os_free(void* mem, uint32_t size)
{
    (void)size;

    if (mem != NULL)
    {
        smac_mem_pool_free(NULL, mem);
    }
}

/// @brief  Create a new event object
/// @details This function creates a new event object with the specified name.
/// @param name Name of the event object
/// @return Handle to the created event object or NULL on failure
smacEventHandle_t smac_event_create(const char* name)
{
    TX_EVENT_FLAGS_GROUP* event = (TX_EVENT_FLAGS_GROUP*)_mem_alloc(sizeof(TX_EVENT_FLAGS_GROUP));

    if (event == NULL)
    {
        return NULL;
    }

    if (tx_event_flags_create(event, (CHAR*)name) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)event);
        return NULL;
    }

    return (smacEventHandle_t)event;
}

/// @brief  Delete an event object
/// @details This function deletes the specified event object and frees its resources.
/// @param event Handle to the event object to be deleted
void smac_event_delete(smacEventHandle_t event)
{
    if (event != NULL)
    {
        tx_event_flags_delete((TX_EVENT_FLAGS_GROUP*)event);
        _mem_free((uint8_t*)event);
    }
}

/// @brief  Get the name of an event object
/// @details This function retrieves the name of the specified event object.
/// @param event Handle to the event object (smacEventHandle_t)
/// @return Pointer to the event object's name string
const char* smac_event_name(smacEventHandle_t event)
{
    char* name                   = NULL;
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);

    return (tx_event_flags_info_get(xevent, (CHAR**)&name, NULL, NULL, NULL, NULL) == TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the current state of an event object
/// @details This function retrieves the current state (flags) of the specified event object.
/// @param event Handle to the event object (smacEventHandle_t)
/// @return Current state (flags) of the event object
uint32_t smac_event_state(smacEventHandle_t event)
{
    uint32_t events_state        = 0;
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);

    return (tx_event_flags_info_get(xevent, NULL, &events_state, NULL, NULL, NULL) == TX_SUCCESS)
               ? events_state
               : 0;
}

/// @brief  Put event flags
/// @details This function puts the specified flags in the event object.
/// @param event Handle to the event object (smacEventHandle_t)
/// @param flags Flags to be putted
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_event_put(smacEventHandle_t event, uint32_t flags)
{
    uint32_t events_state;
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);

    if (tx_event_flags_set(xevent, (ULONG)flags, TX_OR) != TX_SUCCESS)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    if (tx_event_flags_info_get(xevent, NULL, &events_state, NULL, NULL, NULL) != TX_SUCCESS)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    return SMAC_RET_OK;
}

/// @brief  Wait for event flags
/// @details This function waits for the specified flags to be set in the event object.
/// @param event        Handle to the event object (smacEventHandle_t)
/// @param events_value Flags to wait for
/// @param timeout      Timeout in milliseconds to wait (0 for no wait, OS_WAIT_FOREVER for
/// infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_event_wait(smacEventHandle_t event, uint32_t events_value, uint32_t timeout)
{
    ULONG event_value;
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);

    if (events_value == SMAC_OS_EVENT_NONE)
    {
        return SMAC_RET_PARAM_ERR;
    }

    if (tx_event_flags_get(xevent, (ULONG)events_value, TX_OR, &event_value, (ULONG)timeout) !=
        TX_SUCCESS)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    if ((event_value & events_value) == SMAC_OS_EVENT_NONE)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    return SMAC_RET_OK;
}

/// @brief  Clear event flags
/// @details This function clears the specified flags in the event object.
/// @param event Handle to the event object (smacEventHandle_t)
/// @param flags Flags to be cleared
void smac_event_clear(smacEventHandle_t event, uint32_t events_value)
{
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);

    if (events_value != SMAC_OS_EVENT_NONE)
    {
        tx_event_flags_set(xevent, (ULONG)events_value, TX_AND);
    }
}

/// @brief  Wait for event flags and clear them
/// @details This function waits for the specified flags to be set in the event object and clears
/// them upon wakeup.
/// @param event             Handle to the event object (smacEventHandle_t)
/// @param events_value      Flags to wait for
/// @param out_events_value  Pointer to store the flags that caused the wakeup
/// @param timeout           Timeout in milliseconds to wait (0 for no wait, OS_WAIT_FOREVER
/// for infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_event_wait_and_clear(smacEventHandle_t event, uint32_t events_value,
                                        uint32_t* out_events_value, uint32_t timeout)
{
    TX_EVENT_FLAGS_GROUP* xevent = (TX_EVENT_FLAGS_GROUP*)event;

    assert(xevent != NULL);
    assert(xevent->tx_event_flags_group_id == TX_EVENT_FLAGS_ID);
    assert(out_events_value != NULL);

    if (events_value == SMAC_OS_EVENT_NONE)
    {
        return SMAC_RET_PARAM_ERR;
    }

    if (tx_event_flags_get(xevent, (ULONG)events_value, TX_OR, (ULONG*)out_events_value,
                           (ULONG)timeout) != TX_SUCCESS)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    if (((*out_events_value) & events_value) == SMAC_OS_EVENT_NONE)
    {
        return SMAC_RET_OS_EVENT_ERR;
    }

    *out_events_value &= events_value;
    smac_event_clear(event, *out_events_value);

    return SMAC_RET_OK;
}

/// @brief  Create a new message queue
/// @details This function creates a new message queue with the specified parameters.
/// @param name          Name of the message queue
/// @param message_size  Size of each message in bytes
/// @param message_count Maximum number of messages the queue can hold
/// @return Handle to the created message queue or NULL on failure
smacMessageQueueHandle_t smac_message_queue_create(const char* name, uint32_t message_size,
                                                   uint32_t message_count)
{
    TX_QUEUE* queue               = (TX_QUEUE*)_mem_alloc(sizeof(TX_QUEUE));
    uint32_t aligned_message_size = (message_size + sizeof(ULONG) - 1) / sizeof(ULONG);

    if (queue == NULL)
    {
        return NULL;
    }

    VOID* queue_start = (VOID*)_mem_alloc(aligned_message_size * message_count);

    if (queue_start == NULL)
    {
        _mem_free((uint8_t*)queue);
        return NULL;
    }

    if (tx_queue_create(queue, (CHAR*)name, aligned_message_size, queue_start, message_count) !=
        TX_SUCCESS)
    {
        _mem_free((uint8_t*)queue);
        _mem_free((uint8_t*)queue_start);
        return NULL;
    }

    return (smacMessageQueueHandle_t)queue;
}

/// @brief  Create a new message queue with static buffer
/// @details This function creates a new message queue with the specified parameters,
///     using a statically allocated message buffer.
/// @param name           Name of the message queue
/// @param message_buffer Pointer to the statically allocated message buffer
/// @param message_size   Size of each message in bytes
/// @param message_count  Maximum number of messages the queue can hold
/// @return Handle to the created message queue
smacMessageQueueHandle_t smac_message_queue_create_static(const char* name, uint8_t* message_buffer,
                                                          uint32_t message_size,
                                                          uint32_t message_count)
{
    if ((message_buffer == NULL) || (message_size == 0) || (message_count == 0) ||
        (message_size % sizeof(ULONG) != 0))
    {
        return NULL;
    }

    TX_QUEUE* queue               = (TX_QUEUE*)_mem_alloc(sizeof(TX_QUEUE));
    uint32_t aligned_message_size = (message_size + sizeof(ULONG) - 1) / sizeof(ULONG);

    if (queue == NULL)
    {
        return NULL;
    }

    if (tx_queue_create(queue, (CHAR*)name, aligned_message_size, (VOID*)message_buffer,
                        message_count) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)queue);
        return NULL;
    }

    return (smacMessageQueueHandle_t)queue;
}

/// @brief  Delete a message queue
/// @details This function deletes the specified message queue and frees its resources.
/// @param queue Handle to the message queue to be deleted
void smac_message_queue_delete(smacMessageQueueHandle_t queue)
{
    if (queue != NULL)
    {
        _mem_free((uint8_t*)((TX_QUEUE*)queue)->tx_queue_start);
        tx_queue_delete((TX_QUEUE*)queue);
        _mem_free((uint8_t*)queue);
    }
}

/// @brief  Delete a message queue created with static buffer
/// @details This function deletes the specified message queue created with a static buffer.
/// @param queue Handle to the message queue to be deleted
void smac_message_queue_delete_static(smacMessageQueueHandle_t queue)
{
    if (queue != NULL)
    {
        tx_queue_delete((TX_QUEUE*)queue);
        _mem_free((uint8_t*)queue);
    }
}

/// @brief  Get the name of a message queue
/// @param queue Handle to the message queue
/// @return Pointer to the message queue's name string
const char* smac_message_queue_name(smacMessageQueueHandle_t queue)
{
    char* name       = NULL;
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);

    return (tx_queue_info_get(xqueue, (CHAR**)&name, NULL, NULL, NULL, NULL, NULL) == TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the size of a message in the queue
/// @param queue Handle to the message queue
/// @return Size of each message in bytes
uint32_t smac_message_queue_message_size(smacMessageQueueHandle_t queue)
{
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);

    return xqueue->tx_queue_message_size * sizeof(ULONG);
}

/// @brief  Get the number of messages currently in the queue
/// @param queue Handle to the message queue
/// @return Number of messages currently in the queue
uint32_t smac_message_queue_message_count(smacMessageQueueHandle_t queue)
{
    ULONG queued_messages_num = 0U;
    TX_QUEUE* xqueue          = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);

    return (tx_queue_info_get(xqueue, NULL, &queued_messages_num, NULL, NULL, NULL, NULL) ==
            TX_SUCCESS)
               ? (uint32_t)queued_messages_num
               : 0;
}

/// @brief  Get the maximum number of messages the queue can hold
/// @param queue Handle to the message queue
/// @return Maximum number of messages the queue can hold
uint32_t smac_message_queue_max_message_count(smacMessageQueueHandle_t queue)
{
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);

    return (uint32_t)xqueue->tx_queue_capacity;
}

/// @brief  Send a message to the queue
/// @details This function sends a message to the specified message queue.
/// @param queue    Handle to the message queue
/// @param message  Pointer to the buffer to store the received message
/// @param timeout  Timeout in milliseconds to wait for a message (0 for no wait, UINT32_MAX for
/// infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_message_queue_send(smacMessageQueueHandle_t queue, const void* message,
                                      uint32_t timeout)
{
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);
    assert(message != NULL);

    return (tx_queue_send(xqueue, (VOID*)message, (ULONG)timeout) == TX_SUCCESS)
               ? SMAC_RET_OK
               : SMAC_RET_OS_MQ_ERR;
}

/// @brief  Receive a message from the queue
/// @details This function receives a message from the specified message queue.
/// @param queue    Handle to the message queue
/// @param message  Pointer to the buffer to store the received message
/// @param timeout  Timeout in milliseconds to wait for a message (0 for no wait,
/// OS_WAIT_FOREVER for infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_message_queue_receive(smacMessageQueueHandle_t queue, void* message,
                                         uint32_t timeout)
{
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);
    assert(message != NULL);

    return (tx_queue_receive(xqueue, message, (ULONG)timeout) == TX_SUCCESS) ? SMAC_RET_OK
                                                                             : SMAC_RET_OS_MQ_ERR;
}

/// @brief  Clear all messages from the queue
/// @details This function clears all messages currently in the specified message queue.
/// @param queue Handle to the message queue
void smac_message_queue_clear(smacMessageQueueHandle_t queue)
{
    TX_QUEUE* xqueue = (TX_QUEUE*)queue;

    assert(xqueue != NULL);
    assert(xqueue->tx_queue_id == TX_QUEUE_ID);

    tx_queue_flush(xqueue);
}

/// @brief  Create a new memory pool
/// @details This function creates a new memory pool with the specified parameters.
/// @param name        Name of the memory pool
/// @param block_size  Size of each memory block in bytes
/// @param block_count Number of memory blocks in the pool
/// @return Handle to the created memory pool or NULL on failure
smacMemPoolHandle_t smac_mem_pool_create(const char* name, uint32_t block_size,
                                         uint32_t block_count)
{
    uint32_t aligned_block_size = block_size % sizeof(ULONG) == 0
                                      ? block_size
                                      : ((block_size / sizeof(ULONG)) + 1) * sizeof(ULONG);

    TX_BLOCK_POOL* pool = (TX_BLOCK_POOL*)_mem_alloc(sizeof(TX_BLOCK_POOL));

    if (pool == NULL)
    {
        return NULL;
    }

    VOID* pool_zone = _mem_alloc(aligned_block_size * block_count);

    if (pool_zone == NULL)
    {
        _mem_free((uint8_t*)pool);
        return NULL;
    }

    if (tx_block_pool_create(pool, (CHAR*)name, aligned_block_size, pool_zone,
                             aligned_block_size * block_count) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)pool);
        _mem_free((uint8_t*)pool_zone);
        return NULL;
    }

    return (smacMemPoolHandle_t)pool;
}

/// @brief  Create a new memory pool with static buffer
/// @details This function creates a new memory pool with the specified parameters,
///     using a statically allocated pool buffer.
/// @param name        Name of the memory pool
/// @param pool_buffer Pointer to the statically allocated pool buffer
/// @param block_size  Size of each memory block in bytes
/// @param block_count Number of memory blocks in the pool
/// @return Handle to the created memory pool or NULL on failure
smacMemPoolHandle_t smac_mem_pool_create_static(const char* name, void* pool, uint32_t pool_size,
                                                uint8_t* pool_mem, uint32_t block_size,
                                                uint32_t block_count)
{
    if ((pool == NULL) || (pool_size < sizeof(TX_BLOCK_POOL)) || (pool_mem == NULL) ||
        (block_size == 0) || (block_size % sizeof(ULONG) != 0) || (block_count == 0))
    {
        return NULL;
    }

    if (tx_block_pool_create((TX_BLOCK_POOL*)pool, (CHAR*)name, block_size, (VOID*)pool_mem,
                             block_size * block_count) != TX_SUCCESS)
    {
        return NULL;
    }

    return (smacMemPoolHandle_t)pool;
}

/// @brief  Delete a memory pool
/// @details This function deletes the specified memory pool and frees its resources.
/// @param pool Handle to the memory pool to be deleted
void smac_mem_pool_delete(smacMemPoolHandle_t pool)
{
    if ((pool != NULL) && (((TX_BLOCK_POOL*)pool)->tx_block_pool_id != TX_BLOCK_POOL_ID))
    {
        _mem_free((uint8_t*)((TX_BLOCK_POOL*)pool)->tx_block_pool_start);
        tx_block_pool_delete((TX_BLOCK_POOL*)pool);
        _mem_free((uint8_t*)pool);
    }
}

/// @brief  Delete a memory pool created with static buffer
/// @details This function deletes the specified memory pool created with a static buffer.
/// @param pool Handle to the memory pool to be deleted
void smac_mem_pool_delete_static(smacMemPoolHandle_t pool)
{
    if ((pool != NULL) && (((TX_BLOCK_POOL*)pool)->tx_block_pool_id != TX_BLOCK_POOL_ID))
    {
        tx_block_pool_delete((TX_BLOCK_POOL*)pool);
    }
}

/// @brief  Get the name of a memory pool
/// @param pool Handle to the memory pool
/// @return Pointer to the memory pool's name string
const char* smac_mem_pool_name(smacMemPoolHandle_t pool)
{
    char* name           = NULL;
    TX_BLOCK_POOL* xpool = (TX_BLOCK_POOL*)pool;

    assert(xpool != NULL);
    assert(xpool->tx_block_pool_id == TX_BLOCK_POOL_ID);

    return (tx_block_pool_info_get(xpool, (CHAR**)&name, NULL, NULL, NULL, NULL, NULL) ==
            TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the size of each memory block in the pool
/// @param pool Handle to the memory pool
/// @return Size of each memory block in bytes
uint32_t smac_mem_pool_block_size(smacMemPoolHandle_t pool)
{
    TX_BLOCK_POOL* xpool = (TX_BLOCK_POOL*)pool;

    assert(xpool != NULL);
    assert(xpool->tx_block_pool_id == TX_BLOCK_POOL_ID);

    return (uint32_t)xpool->tx_block_pool_block_size;
}

/// @brief  Get the number of used memory blocks in the pool
/// @param pool Handle to the memory pool
/// @return Number of memory blocks in the pool
uint32_t smac_mem_pool_block_count(smacMemPoolHandle_t pool)
{
    ULONG available_blocks = 0U;
    ULONG total_blocks     = 0U;
    TX_BLOCK_POOL* xpool   = (TX_BLOCK_POOL*)pool;

    assert(xpool != NULL);
    assert(xpool->tx_block_pool_id == TX_BLOCK_POOL_ID);

    return (tx_block_pool_info_get(xpool, NULL, &available_blocks, &total_blocks, NULL, NULL,
                                   NULL) != TX_SUCCESS)
               ? 0
               : (uint32_t)(total_blocks - available_blocks);
}

/// @brief  Get the maximum number of memory blocks in the pool
/// @param pool Handle to the memory pool
/// @return Maximum number of memory blocks in the pool
uint32_t smac_mem_pool_max_block_count(smacMemPoolHandle_t pool)
{
    ULONG total_blocks   = 0U;
    TX_BLOCK_POOL* xpool = (TX_BLOCK_POOL*)pool;

    assert(xpool != NULL);
    assert(xpool->tx_block_pool_id == TX_BLOCK_POOL_ID);

    return (tx_block_pool_info_get(xpool, NULL, NULL, &total_blocks, NULL, NULL, NULL) !=
            TX_SUCCESS)
               ? 0
               : (uint32_t)total_blocks;
}

/// @brief  Allocate a memory block from the pool
/// @details This function allocates a memory block from the specified memory pool.
/// @param pool    Handle to the memory pool
/// @param timeout Timeout in milliseconds to wait for a memory block (0 for no wait,
/// OS_WAIT_FOREVER for infinite wait)
/// @return Pointer to the allocated memory block, or NULL on failure
void* smac_mem_pool_alloc(smacMemPoolHandle_t pool, uint32_t timeout)
{
    VOID* mem            = NULL;
    TX_BLOCK_POOL* xpool = (TX_BLOCK_POOL*)pool;

    assert(xpool != NULL);
    assert(xpool->tx_block_pool_id == TX_BLOCK_POOL_ID);

    return (tx_block_allocate(xpool, &mem, (ULONG)timeout) == TX_SUCCESS) ? mem : NULL;
}

/// @brief  Free a memory block back to the pool
/// @details This function frees a previously allocated memory block back to the specified memory
/// pool.
/// @param pool  Handle to the memory pool
/// @param block Pointer to the memory block to be freed
void smac_mem_pool_free(smacMemPoolHandle_t pool, void* block)
{
    (void)pool;

    if (block != NULL)
    {
        tx_block_release(block);
    }
}

/// @brief  Create a new mutex
/// @details This function creates a new mutex with the specified name.
/// @param name Name of the mutex
/// @return Handle to the created mutex or NULL on failure
smacMutexHandle_t smac_mutex_create(const char* name)
{
    TX_MUTEX* mutex = (TX_MUTEX*)_mem_alloc(sizeof(TX_MUTEX));

    if (mutex == NULL)
    {
        return NULL;
    }

    if (tx_mutex_create(mutex, (CHAR*)name, TX_INHERIT) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)mutex);
        return NULL;
    }

    return (smacMutexHandle_t)mutex;
}

/// @brief  Delete a mutex
/// @details This function deletes the specified mutex and frees its resources.
/// @param mutex Handle to the mutex to be deleted
void smac_mutex_delete(smacMutexHandle_t mutex)
{
    if (mutex != NULL)
    {
        tx_mutex_delete((TX_MUTEX*)mutex);
        _mem_free((uint8_t*)mutex);
    }
}

/// @brief  Get the name of a mutex
/// @param mutex Handle to the mutex
/// @return Pointer to the mutex's name string
const char* smac_mutex_name(smacMutexHandle_t mutex)
{
    char* name       = NULL;
    TX_MUTEX* xmutex = (TX_MUTEX*)mutex;

    assert(xmutex != NULL);
    assert(xmutex->tx_mutex_id == TX_MUTEX_ID);

    return (tx_mutex_info_get(xmutex, (CHAR**)&name, NULL, NULL, NULL, NULL, NULL) == TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the owner of a mutex
/// @details This function retrieves the handle of the task that currently owns the specified
/// mutex.
/// @param mutex Handle to the mutex
/// @return Handle to the task that currently owns the mutex
smacTaskHandle_t smac_mutex_owner(smacMutexHandle_t mutex)
{
    TX_THREAD* owner = NULL;
    TX_MUTEX* xmutex = (TX_MUTEX*)mutex;

    assert(xmutex != NULL);
    assert(xmutex->tx_mutex_id == TX_MUTEX_ID);

    return (tx_mutex_info_get(xmutex, NULL, NULL, &owner, NULL, NULL, NULL) == TX_SUCCESS)
               ? (smacTaskHandle_t)owner
               : NULL;
}

/// @brief  Lock a mutex
/// @details This function locks the specified mutex, blocking the calling task if necessary.
/// @param mutex   Handle to the mutex
/// @param timeout Timeout in milliseconds to wait for the mutex (0 for no wait,
/// OS_WAIT_FOREVER for infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_mutex_lock(smacMutexHandle_t mutex, uint32_t timeout)
{
    TX_MUTEX* xmutex = (TX_MUTEX*)mutex;

    assert(xmutex != NULL);
    assert(xmutex->tx_mutex_id == TX_MUTEX_ID);

    return (tx_mutex_get(xmutex, (ULONG)timeout) == TX_SUCCESS) ? SMAC_RET_OK
                                                                : SMAC_RET_OS_MUTEX_ERR;
}

/// @brief  Unlock a mutex
/// @details This function unlocks the specified mutex.
/// @param mutex Handle to the mutex
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_mutex_unlock(smacMutexHandle_t mutex)
{
    TX_MUTEX* xmutex = (TX_MUTEX*)mutex;

    assert(xmutex != NULL);
    assert(xmutex->tx_mutex_id == TX_MUTEX_ID);

    return (tx_mutex_put(xmutex) == TX_SUCCESS) ? SMAC_RET_OK : SMAC_RET_OS_MUTEX_ERR;
}

/// @brief  Create a new semaphore
/// @details This function creates a new semaphore with the specified parameters.
/// @param name          Name of the semaphore
/// @param max_count     Maximum count of the semaphore
/// @return Handle to the created semaphore, or NULL on failure
smacSemaphoreHandle_t smac_semaphore_create(const char* name, uint32_t max_count)
{
    TX_SEMAPHORE* semaphore = (TX_SEMAPHORE*)_mem_alloc(sizeof(TX_SEMAPHORE));

    if (semaphore == NULL)
    {
        return NULL;
    }

    if (tx_semaphore_create(semaphore, (CHAR*)name, (ULONG)max_count) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)semaphore);
        return NULL;
    }

    return (smacSemaphoreHandle_t)semaphore;
}

/// @brief  Delete a semaphore
/// @details This function deletes the specified semaphore and frees its resources.
/// @param semaphore Handle to the semaphore to be deleted
void smac_semaphore_delete(smacSemaphoreHandle_t semaphore)
{
    if (semaphore != NULL)
    {
        tx_semaphore_delete((TX_SEMAPHORE*)semaphore);
        _mem_free((uint8_t*)semaphore);
    }
}

/// @brief  Get the name of a semaphore
/// @param semaphore Handle to the semaphore
/// @return Pointer to the semaphore's name string
const char* smac_semaphore_name(smacSemaphoreHandle_t semaphore)
{
    char* name               = NULL;
    TX_SEMAPHORE* xsemaphore = (TX_SEMAPHORE*)semaphore;

    assert(xsemaphore != NULL);
    assert(xsemaphore->tx_semaphore_id == TX_SEMAPHORE_ID);

    return (tx_semaphore_info_get(xsemaphore, (CHAR**)&name, NULL, NULL, NULL, NULL) == TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the current count of a semaphore
/// @param semaphore Handle to the semaphore
/// @return Current count of the semaphore
uint32_t smac_semaphore_count(smacSemaphoreHandle_t semaphore)
{
    ULONG current_count      = 0U;
    TX_SEMAPHORE* xsemaphore = (TX_SEMAPHORE*)semaphore;

    assert(xsemaphore != NULL);
    assert(xsemaphore->tx_semaphore_id == TX_SEMAPHORE_ID);

    return (tx_semaphore_info_get(xsemaphore, NULL, &current_count, NULL, NULL, NULL) == TX_SUCCESS)
               ? (uint32_t)current_count
               : 0;
}

/// @brief  Take (decrement) a semaphore
/// @details This function takes (decrements) the specified semaphore, blocking the calling task
/// if necessary.
/// @param semaphore Handle to the semaphore
/// @param timeout   Timeout in milliseconds to wait for the semaphore (0 for no wait,
/// OS_WAIT_FOREVER for infinite wait)
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_semaphore_take(smacSemaphoreHandle_t semaphore, uint32_t timeout)
{
    TX_SEMAPHORE* xsemaphore = (TX_SEMAPHORE*)semaphore;

    assert(xsemaphore != NULL);
    assert(xsemaphore->tx_semaphore_id == TX_SEMAPHORE_ID);

    return (tx_semaphore_get(xsemaphore, (ULONG)timeout) == TX_SUCCESS) ? SMAC_RET_OK
                                                                        : SMAC_RET_OS_SEMAPHORE_ERR;
}

/// @brief  Release (increment) a semaphore
/// @details This function releases (increments) the specified semaphore.
/// @param semaphore Handle to the semaphore
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_semaphore_release(smacSemaphoreHandle_t semaphore)
{
    TX_SEMAPHORE* xsemaphore = (TX_SEMAPHORE*)semaphore;

    assert(xsemaphore != NULL);
    assert(xsemaphore->tx_semaphore_id == TX_SEMAPHORE_ID);

    return (tx_semaphore_put(xsemaphore) == TX_SUCCESS) ? SMAC_RET_OK : SMAC_RET_OS_SEMAPHORE_ERR;
}

/// @brief  Create a new task
/// @details This function creates a new task with the specified parameters.
///     The task will start executing the provided main function with the given argument.
/// @param name        Name of the task
/// @param main        Pointer to the task's main function
/// @param arg         Argument to be passed to the task's main function
/// @param stack_size  Size of the task's stack in bytes
/// @param priority    Priority of the task (use OS_TASK_PRIORITY_* constants)
/// @return Handle to the created task, or NULL on failure
smacTaskHandle_t smac_task_create(const char* name, void (*main)(void*), void* arg,
                                  uint32_t stack_size, smacTaskPriority_t priority)
{
    assert(main != NULL);
    assert(stack_size >= TX_MINIMUM_STACK);

    TX_THREAD* thread           = (TX_THREAD*)_mem_alloc(sizeof(TX_THREAD));
    uint32_t aligned_stack_size = (stack_size + sizeof(ULONG) - 1) / sizeof(ULONG);

    if (thread == NULL)
    {
        return NULL;
    }

    VOID* stack = _mem_alloc(stack_size);

    if (stack == NULL)
    {
        _mem_free((uint8_t*)thread);
        return NULL;
    }

    UINT converted_priority = (UINT)(SMAC_TASK_PRIORITY_MAX - priority);

    if (tx_thread_create(thread, (CHAR*)name, (void (*)(ULONG))main, (ULONG)arg, stack,
                         aligned_stack_size, (UINT)converted_priority, converted_priority,
                         OS_DEFAULT_TIME_SLICE, TX_AUTO_START) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)stack);
        _mem_free((uint8_t*)thread);
        return NULL;
    }

    return (smacTaskHandle_t)thread;
}

/// @brief  Create a new task with static stack allocation
/// @details This function creates a new task with the specified parameters.
///     The task will start executing the provided main function with the given argument.
/// @param name        Name of the task
/// @param main        Pointer to the task's main function
/// @param arg         Argument to be passed to the task's main function
/// @param stack       Pointer to the static stack buffer
/// @param stack_size  Size of the task's stack in bytes
/// @param priority    Priority of the task (use OS_TASK_PRIORITY_* constants)
/// @return Handle to the created task, or NULL on failure
smacTaskHandle_t smac_task_create_static(const char* name, void (*main)(void*), void* arg,
                                         uint8_t* stack, uint32_t stack_size,
                                         smacTaskPriority_t priority)
{
    assert(main != NULL);
    assert(stack != NULL);
    assert(stack_size >= TX_MINIMUM_STACK);

    if (stack_size % sizeof(ULONG) != 0)
    {
        return NULL;
    }

    TX_THREAD* thread           = (TX_THREAD*)_mem_alloc(sizeof(TX_THREAD));
    uint32_t aligned_stack_size = (stack_size + sizeof(ULONG) - 1) / sizeof(ULONG);

    if (thread == NULL)
    {
        return NULL;
    }

    UINT converted_priority = (UINT)(SMAC_TASK_PRIORITY_MAX - priority);

    if (tx_thread_create(thread, (CHAR*)name, (void (*)(ULONG))main, (ULONG)arg, (VOID*)stack,
                         aligned_stack_size, (UINT)converted_priority, converted_priority,
                         OS_DEFAULT_TIME_SLICE, TX_AUTO_START) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)thread);
        return NULL;
    }

    return (smacTaskHandle_t)thread;
}

/// @brief  Delete a task
/// @details This function deletes the specified task and frees its resources.
/// @param task Handle to the task to be deleted
void smac_task_delete(smacTaskHandle_t task)
{
    if ((task != NULL) && (((TX_THREAD*)task)->tx_thread_id == TX_THREAD_ID))
    {
        if (tx_thread_terminate((TX_THREAD*)task) == TX_SUCCESS)
        {
            _mem_free((uint8_t*)((TX_THREAD*)task)->tx_thread_stack_start);
            tx_thread_delete((TX_THREAD*)task);
            _mem_free((uint8_t*)task);
        }
    }
}

/// @brief  Delete a task created with static stack allocation
/// @details This function deletes the specified task created with static stack allocation.
/// @param task Handle to the task to be deleted
void smac_task_delete_static(smacTaskHandle_t task)
{
    if ((task != NULL) && (((TX_THREAD*)task)->tx_thread_id == TX_THREAD_ID))
    {
        if (tx_thread_terminate((TX_THREAD*)task) == TX_SUCCESS)
        {
            tx_thread_delete((TX_THREAD*)task);
            _mem_free((uint8_t*)task);
        }
    }
}

/// @brief  Get the name of a task
/// @param task Handle to the task control block
/// @return Pointer to the task's name string
const char* smac_task_name(smacTaskHandle_t task)
{
    char* name       = NULL;
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    return (tx_thread_info_get((TX_THREAD*)task, (CHAR**)&name, NULL, NULL, NULL, NULL, NULL, NULL,
                               NULL) == TX_SUCCESS)
               ? name
               : NULL;
}

/// @brief  Get the current state of a task
/// @param task Handle to the task control block
/// @return Current state of the task
uint32_t smac_task_stack_size(smacTaskHandle_t task)
{
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    return (uint32_t)xtask->tx_thread_stack_size;
}

/// @brief  Get the priority of a task
/// @param task Handle to the task control block
/// @return Priority of the task
smacTaskPriority_t smac_task_priority(smacTaskHandle_t task)
{
    TX_THREAD* xtask = (TX_THREAD*)task;
    UINT priority    = SMAC_TASK_PRIORITY_NONE;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    return (tx_thread_info_get((TX_THREAD*)task, NULL, NULL, NULL, &priority, NULL, NULL, NULL,
                               NULL) == TX_SUCCESS)
               ? _convert_threadx_task_priority(priority)
               : SMAC_TASK_PRIORITY_NONE;
}

/// @brief  Get the current state of a task
/// @param task Handle to the task control block
/// @return Current state of the task
smacTaskState_t smac_task_state(smacTaskHandle_t task)
{
    UINT state;
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    if (smac_os_current_task() == task)
    {
        return SMAC_TASK_STATE_RUNNING;
    }

    return tx_thread_info_get((TX_THREAD*)task, NULL, &state, NULL, NULL, NULL, NULL, NULL, NULL) ==
                   TX_SUCCESS
               ? _convert_threadx_task_state(state)
               : SMAC_TASK_STATE_UNKNOWN;
}

/// @brief  Set the priority of a task
/// @param task     Handle to the task control block
/// @param priority New priority to be set for the task
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_task_set_priority(smacTaskHandle_t task, smacTaskPriority_t priority)
{
    UINT current_priority;
    UINT converted_priority;
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    converted_priority = (UINT)(SMAC_TASK_PRIORITY_MAX - priority);

    return (tx_thread_priority_change((TX_THREAD*)task, converted_priority, &current_priority) ==
            TX_SUCCESS)
               ? SMAC_RET_OK
               : SMAC_RET_OS_TASK_ERR;
}

/// @brief  Suspend a task
/// @param task Handle to the task control block to be suspended
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_task_suspend(smacTaskHandle_t task)
{
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    return (tx_thread_suspend(xtask) == TX_SUCCESS) ? SMAC_RET_OK : SMAC_RET_OS_TASK_ERR;
}

/// @brief  Resume a suspended task
/// @param task Handle to the task control block to be resumed
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_task_resume(smacTaskHandle_t task)
{
    TX_THREAD* xtask = (TX_THREAD*)task;

    assert(xtask != NULL);
    assert(xtask->tx_thread_id == TX_THREAD_ID);

    return (tx_thread_resume(xtask) == TX_SUCCESS) ? SMAC_RET_OK : SMAC_RET_OS_TASK_ERR;
}

/// @brief  Create a new timer
/// @details This function creates a new timer with the specified parameters.
/// @param name        Name of the timer
/// @param callback    Pointer to the timer's callback function
/// @param arg         Argument to be passed to the timer's callback function
/// @return Handle to the created timer, or NULL on failure
smacTimerHandle_t smac_timer_create_once(const char* name, void (*callback)(void*), void* arg)
{
    assert(callback != NULL);

    TX_TIMER* timer = (TX_TIMER*)_mem_alloc(sizeof(TX_TIMER));

    if (timer == NULL)
    {
        return NULL;
    }

    if (tx_timer_create(timer, (CHAR*)name, (void (*)(ULONG))callback, (ULONG)arg, 1, 0,
                        TX_NO_ACTIVATE) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)timer);
        return NULL;
    }

    return (smacTimerHandle_t)timer;
}

/// @brief  Create a new periodic timer
/// @details This function creates a new periodic timer with the specified parameters.
/// @param name        Name of the timer
/// @param callback    Pointer to the timer's callback function
/// @param arg         Argument to be passed to the timer's callback function
/// @return Handle to the created periodic timer, or NULL on failure
smacTimerHandle_t smac_timer_create_periodic(const char* name, void (*callback)(void*), void* arg)
{
    assert(callback != NULL);

    TX_TIMER* timer = (TX_TIMER*)_mem_alloc(sizeof(TX_TIMER));

    if (timer == NULL)
    {
        return NULL;
    }

    if (tx_timer_create(timer, (CHAR*)name, (void (*)(ULONG))callback, (ULONG)arg, 1, 1,
                        TX_NO_ACTIVATE) != TX_SUCCESS)
    {
        _mem_free((uint8_t*)timer);
        return NULL;
    }

    return (smacTimerHandle_t)timer;
}

/// @brief  Delete a timer
/// @details This function deletes the specified timer and frees its resources.
/// @param timer Handle to the timer to be deleted
void smac_timer_delete(smacTimerHandle_t timer)
{
    if (timer != NULL)
    {
        tx_timer_delete((TX_TIMER*)timer);
        _mem_free((uint8_t*)timer);
    }
}

/// @brief  Get the name of a timer
/// @param timer Handle to the timer
/// @return Pointer to the timer's name string
const char* smac_timer_name(smacTimerHandle_t timer)
{
    char* name       = NULL;
    TX_TIMER* xtimer = (TX_TIMER*)timer;

    assert(xtimer != NULL);
    assert(xtimer->tx_timer_id == TX_TIMER_ID);

    return (tx_timer_info_get(xtimer, (CHAR**)&name, NULL, NULL, NULL, NULL) == TX_SUCCESS) ? name
                                                                                            : NULL;
}

/// @brief  Get the current state of a timer
/// @param timer Handle to the timer
/// @return Current state of the timer
smacTimerState_t smac_timer_state(smacTimerHandle_t timer)
{
    UINT active;
    TX_TIMER* xtimer = (TX_TIMER*)timer;

    assert(xtimer != NULL);
    assert(xtimer->tx_timer_id == TX_TIMER_ID);

    if (tx_timer_info_get(xtimer, NULL, &active, NULL, NULL, NULL) != TX_SUCCESS)
    {
        return SMAC_TIMER_STATE_UNKNOWN;
    }

    return (active) ? SMAC_TIMER_STATE_ACTIVE : SMAC_TIMER_STATE_IDLE;
}

/// @brief  Start a timer
/// @details This function starts the specified timer with the given timeout.
/// @param timer   Handle to the timer
/// @param timeout Timeout in OS count for the timer
/// @return SMAC_RET_OK on success, error code otherwise
smacRetCode_t smac_timer_start(smacTimerHandle_t timer, uint32_t timeout)
{
    UINT active;
    TX_TIMER* xtimer = (TX_TIMER*)timer;

    assert(xtimer != NULL);
    assert(xtimer->tx_timer_id == TX_TIMER_ID);

    if (tx_timer_info_get(xtimer, NULL, &active, NULL, NULL, NULL) != TX_SUCCESS)
    {
        return SMAC_RET_OS_TIMER_ERR;
    }

    if (active)
    {
        if (tx_timer_deactivate(xtimer) != TX_SUCCESS)
        {
            return SMAC_RET_OS_TIMER_ERR;
        }
    }

    if (tx_timer_change(xtimer, (ULONG)timeout, (ULONG)timeout) != TX_SUCCESS)
    {
        return SMAC_RET_OS_TIMER_ERR;
    }

    if (tx_timer_activate(xtimer) != TX_SUCCESS)
    {
        return SMAC_RET_OS_TIMER_ERR;
    }

    return SMAC_RET_OK;
}

/// @brief  Stop a timer
/// @details This function stops the specified timer.
/// @param timer Handle to the timer
void smac_timer_stop(smacTimerHandle_t timer)
{
    UINT active;
    TX_TIMER* xtimer = (TX_TIMER*)timer;

    assert(xtimer != NULL);
    assert(xtimer->tx_timer_id == TX_TIMER_ID);

    if (tx_timer_info_get(xtimer, NULL, &active, NULL, NULL, NULL) == TX_SUCCESS)
    {
        if (active)
        {
            tx_timer_deactivate(xtimer);
        }
    }
}
