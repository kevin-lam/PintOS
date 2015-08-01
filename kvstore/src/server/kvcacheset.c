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
static bool key_in_cacheset(kvcacheset_t *cacheset, char * key);
static void change_entry_value(kvcacheset_t *cacheset, char * key, char * new_value);
static struct kvcacheentry * create_kvcacheentry(char * key, char * value);
static bool kvcacheset_at_capacity(kvcacheset_t *cacheset);

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
  if (strlen(key) >= MAX_KEYLEN) {
    return ERRKEYLEN;
  }
  struct kvcacheentry * entry;
  HASH_FIND_STR(cacheset->entries, key, entry);
  if (NULL == entry) {
    return ERRNOKEY;
  }
  entry->refbit = true;
  *value = malloc(strlen(entry->value) + 1);
  if (NULL == *value) {
    return -1;
  }
  strcpy(*value, entry->value);
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {

  // I HAVE NOT IMPLEMENTED THE LOCK YET

  assert(cacheset);
  assert(key);
  assert(value);
  if (strlen(key) >= MAX_KEYLEN) {
    return ERRKEYLEN;
  }
  if (strlen(value) >= MAX_VALLEN) {
    return ERRVALLEN;
  }

  if (key_in_cacheset(cacheset, key)) {
    change_entry_value(cacheset, key, value);
    return 0;
  }

  struct kvcacheentry * entry = create_kvcacheentry(key, value);
  assert(NULL != entry);

  if (kvcacheset_at_capacity(cacheset)) {
    kvcacheset_evict(cacheset, entry); 
  }

  DL_APPEND(cacheset->eviction_queue, entry);
  HASH_ADD_KEYPTR(hh, cacheset->entries, entry->key, strlen(entry->key), entry);
  cacheset->num_entries++;

  return 0;
}

static void 
kvcacheset_evict(kvcacheset_t *cacheset, struct kvcacheentry *entry) {
  assert(cacheset);
  assert(entry);
  struct kvcacheentry * victim = NULL;
  struct kvcacheentry * tmp = NULL;
  victim = cacheset->entries;
  while (1) {
    if (NULL == victim) {
      printf("We couldn't find a victim to evict due to an error.\n");
      return;
    }
    if (!victim->refbit) {
      break;
    }
    victim->refbit = false;
    tmp = victim;
    victim = victim->next;
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
  if (strlen(key) >= MAX_KEYLEN) {
    return ERRKEYLEN;
  }

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

static bool 
key_in_cacheset(kvcacheset_t *cacheset, char * key) {
  struct kvcacheentry *tmp;
  HASH_FIND_STR(cacheset->entries, key, tmp);
  return (NULL != tmp);
}

static void 
change_entry_value(kvcacheset_t *cacheset, char * key, char * new_value) {
  struct kvcacheentry *tmp;
  HASH_FIND_STR(cacheset->entries, key, tmp);
  assert(tmp);
  free(tmp->value);
  tmp->value = malloc(strlen(new_value) + 1);
  strcpy(tmp->value, new_value);
}

static struct kvcacheentry *
create_kvcacheentry(char * key, char * value) {
  struct kvcacheentry * entry = malloc(sizeof(struct kvcacheentry));
  if (NULL == entry) {
    return NULL;
  }
  entry->key = malloc(sizeof(char) * strlen(key) + 1);
  if (NULL == entry->key) {
    return NULL;
  }
  strcpy(entry->key, key);

  entry->value = malloc(sizeof(char) * strlen(value) + 1);
  if (NULL == entry->value) {
    return NULL;
  }
  strcpy(entry->value, value);
  entry->refbit = false;
  entry->id = hash(key);
  return entry;
}

static bool 
kvcacheset_at_capacity(kvcacheset_t *cacheset) {
  assert(cacheset);
  return cacheset->num_entries == cacheset->elem_per_set;
}
