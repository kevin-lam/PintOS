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
    // better error code?
    return -1;
  }
  *value = entry->value;
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  // create hash entry
  // insert hash entry into table.
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

  entry->refbit = true;
  entry->id = hash(key);

  if (cacheset->num_entries == cacheset->elem_per_set) {
    printf("Replacement policy not implemented\n");
    return -1;
  }

  // I have not implemented the lock yet

  // see: https://troydhanson.github.io/uthash/userguide.html
  HASH_ADD_KEYPTR(hh, cacheset->entries, entry->key, strlen(entry->key), entry);
  cacheset->num_entries++;

  // should I be using one of the macros?
  return 0;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  return -1;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
}
