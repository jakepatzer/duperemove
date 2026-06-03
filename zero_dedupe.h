/*
 * zero_dedupe.h
 *
 * --zero-only-dedupe mode: walks the cmdline paths, detects contiguous
 * runs of all-zero data, and FIDEDUPERANGE-batches them against a
 * rotating canonical zero extent. No hashfile, no DB, no h16, no
 * candidate lookup - bounded constant memory regardless of corpus
 * size.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#ifndef __ZERO_DEDUPE_H__
#define __ZERO_DEDUPE_H__

int zero_only_dedupe_main(int argc, char **argv, int filelist_idx);

#endif	/* __ZERO_DEDUPE_H__ */
