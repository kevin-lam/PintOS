#include <pthread.h>
#include <errno.h>
#include <stdbool.h>
#include "uthash.h"
#include "utlist.h"
#include "kvconstants.h"
#include "kvcacheset.h"
#include "kvstore.h"
#include <string.h>
#include <assert.h>
#include <stdio.h>

static void kvcacheset_evict(kvcacheset_t *cacheset, struct kvcacheentry *entry);

/* Initializes CACHESET to hold a maximum of ELEM_PER_SET elements.
 * ELEM_PER_SET must be at least 2.
 * Returns 0 if successful, else a negative error code. */
int kvcacheset_init(kvcacheset_t *cacheset, unsigned int elem_per_set) {
  int ret;
  if (elem_per_set < 2)
    return -1;
  cacheset->elem_per_set = elem_per_set;
  if ((ret = pthread_rwlock_init(&cacheset->lock, NULL)) < 0)
    return ret;
  cacheset->num_entries = 0;
  cacheset->entries = NULL;
  cacheset->eviction_queue = NULL;
  return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  assert(cacheset);
  assert(key);
  struct kvcacheentry * entry;
  HASH_FIND_STR(cacheset->entries, key, entry);
  if (NULL == entry) {
    return ERRNOKEY;
  }
  entry->refbit = true;
  *value = malloc(strlen(entry->value) + 1);
  if (NULL == *value) {
    // better error message?
    return -1;
  }
  strcpy(*value, entry->value);
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  assert(cacheset);
  assert(key);
  assert(value);

  // if the entry is present then we modify it
  struct kvcacheentry *tmp;
  HASH_FIND_STR(cacheset->entries, key, tmp);
  if (NULL != tmp) {
    free(tmp->value);
    tmp->value = malloc(strlen(value) + 1);
    if (NULL == tmp->value) {
      // some error code?
      return -1;
    }
    strcpy(tmp->value, value);
    return 0;
  }

  // the entry is not present, so we create one
  struct kvcacheentry * entry = malloc(sizeof(struct kvcacheentry));
  if (NULL == entry) {
    // should look into macros for error code
    return -1;
  }
  entry->key = malloc(sizeof(char) * strlen(key) + 1);
  if (NULL == entry->key) {
    // should look into macros for error code
    return -1;
  }
  strcpy(entry->key, key);

  entry->value = malloc(sizeof(char) * strlen(value) + 1);
  if (NULL == entry->value) {
    // should look into macros for error code
    return -1;
  }
  strcpy(entry->value, value);
  entry->refbit = false;
  entry->id = hash(key);

  // do we need to evict an item?
  if (cacheset->num_entries == cacheset->elem_per_set) {
    kvcacheset_evict(cacheset, entry); 
  }

  DL_APPEND(cacheset->eviction_queue, entry);

  // I have not implemented the lock yet

  // see: https://troydhanson.github.io/uthash/userguide.html
  HASH_ADD_KEYPTR(hh, cacheset->entries, entry->key, strlen(entry->key), entry);
  cacheset->num_entries++;

  return 0;
}

static void 
kvcacheset_evict(kvcacheset_t *cacheset, struct kvcacheentry *entry) {
  assert(cacheset);
  assert(entry);
  // do something
  struct kvcacheentry * victim = NULL;
  struct kvcacheentry * tmp = NULL;
  victim = cacheset->entries;
  while (1) {
    if (NULL == victim) {
      // panic
      printf("our victim is null\n");
      return;
    }
    if (!victim->refbit) {
      break;
    }
    tmp = victim;
    victim = victim->next;
    tmp->refbit = false;
    DL_DELETE(cacheset->entries, tmp);
    DL_APPEND(cacheset->entries, tmp);
  }
  DL_DELETE(cacheset->entries, victim);
  HASH_DEL(cacheset->entries, victim);
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  assert(cacheset);
  assert(key);
  struct kvcacheentry * entry;
  HASH_FIND_STR(cacheset->entries, key, entry);
  if (NULL == entry) {
    return ERRNOKEY;
  }
  HASH_DEL(cacheset->entries, entry);
  DL_DELETE(cacheset->eviction_queue, entry);
  free(entry->key);
  free(entry->value);
  free(entry);
  cacheset->num_entries--;
  return 0;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
  assert(cacheset);
  struct kvcacheentry * current_entry = NULL;
  struct kvcacheentry * tmp = NULL;
  DL_FOREACH_SAFE(cacheset->eviction_queue, current_entry, tmp) {
    DL_DELETE(cacheset->eviction_queue, current_entry);
  }
  HASH_ITER(hh, cacheset->entries, current_entry, tmp) {
    HASH_DEL(cacheset->entries,current_entry);  
    free(current_entry->key);     
    free(current_entry->value);  
    free(current_entry);        
  }
  cacheset->num_entries = 0;
}
