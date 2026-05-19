#ifndef	__RUN_DEDUPE_H__
#define	__RUN_DEDUPE_H__

#include <stdbool.h>
#include <stdint.h>
#include "opt.h"

struct filerec;
struct dedupe_ctxt;

void print_dupes_table(struct results_tree *res, bool whole_file);
void dedupe_results(struct results_tree *res, bool whole_file);

int fdupes_dedupe(void);

/*
 * Coalesce range-extension primitives. Shared with the streaming
 * --lookup-only path so that the same byte-comparison extension and
 * (src_id, dst_id) high-water suppression apply uniformly. See
 * run_dedupe.c for full documentation.
 */
uint64_t extend_match(struct filerec *tgt, uint64_t tgt_off,
		      struct filerec *dst, uint64_t dst_off,
		      uint64_t seed_len);
void coalesce_map_init(void);
void coalesce_map_destroy(void);
bool coalesce_covered(int64_t src_id, int64_t dst_id, uint64_t dst_off);
void coalesce_record(int64_t src_id, int64_t dst_id, uint64_t dst_end);
void process_dedupe_results(struct dedupe_ctxt *ctxt, uint64_t *kern_bytes);

#endif	/* __RUN_DEDUPE_H__ */
