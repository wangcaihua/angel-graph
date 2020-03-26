package com.tencent.angel.graph.utils;

import java.util.Comparator;

class TimSort<K, Buffer> {

  private static final int MIN_MERGE = 32;

  private final SortDataFormat<K, Buffer> s;

  public TimSort(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
  }

  public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert c != null;

    int nRemaining  = hi - lo;
    if (nRemaining < 2)
      return;  // Arrays of size 0 and 1 are always sorted

    // If array is small, do a "mini-TimSort" with no merges
    if (nRemaining < MIN_MERGE) {
      int initRunLen = countRunAndMakeAscending(a, lo, hi, c);
      binarySort(a, lo, hi, lo + initRunLen, c);
      return;
    }

    SortState sortState = new SortState(a, c, hi - lo);
    int minRun = minRunLength(nRemaining);
    do {
      // Identify next run
      int runLen = countRunAndMakeAscending(a, lo, hi, c);

      // If run is short, extend to min(minRun, nRemaining)
      if (runLen < minRun) {
        int force = nRemaining <= minRun ? nRemaining : minRun;
        binarySort(a, lo, lo + force, lo + runLen, c);
        runLen = force;
      }

      // Push run onto pending-run stack, and maybe merge
      sortState.pushRun(lo, runLen);
      sortState.mergeCollapse();

      // Advance to find next run
      lo += runLen;
      nRemaining -= runLen;
    } while (nRemaining != 0);

    // Merge all remaining runs to complete sort
    assert lo == hi;
    sortState.mergeForceCollapse();
    assert sortState.stackSize == 1;
  }

  @SuppressWarnings("fallthrough")
  private void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;

    K key0 = s.newKey();
    K key1 = s.newKey();

    Buffer pivotStore = s.allocate(1);
    for ( ; start < hi; start++) {
      s.copyElement(a, start, pivotStore, 0);
      K pivot = s.getKey(pivotStore, 0, key0);

      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (c.compare(pivot, s.getKey(a, mid, key1)) < 0)
          right = mid;
        else
          left = mid + 1;
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left;  // The number of elements to move
      // Switch is just an optimization for arraycopy in default case
      switch (n) {
        case 2:  s.copyElement(a, left + 1, a, left + 2);
        case 1:  s.copyElement(a, left, a, left + 1);
          break;
        default: s.copyRange(a, left, a, left + 1, n);
      }
      s.copyElement(pivotStore, 0, a, left);
    }
  }

  private int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi)
      return 1;

    K key0 = s.newKey();
    K key1 = s.newKey();

    // Find end of run, and reverse range if descending
    if (c.compare(s.getKey(a, runHi++, key0), s.getKey(a, lo, key1)) < 0) { // Descending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) < 0)
        runHi++;
      reverseRange(a, lo, runHi);
    } else {                              // Ascending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) >= 0)
        runHi++;
    }

    return runHi - lo;
  }

  private void reverseRange(Buffer a, int lo, int hi) {
    hi--;
    while (lo < hi) {
      s.swap(a, lo, hi);
      lo++;
      hi--;
    }
  }

  private int minRunLength(int n) {
    assert n >= 0;
    int r = 0;      // Becomes 1 if any 1 bits are shifted off
    while (n >= MIN_MERGE) {
      r |= (n & 1);
      n >>= 1;
    }
    return n + r;
  }

  private class SortState {
    private final Buffer a;
    private final int aLength;
    private final Comparator<? super K> c;
    private static final int  MIN_GALLOP = 7;
    private int minGallop = MIN_GALLOP;
    private static final int INITIAL_TMP_STORAGE_LENGTH = 256;
    private Buffer tmp; // Actual runtime type will be Object[], regardless of T
    private int tmpLength = 0;
    private int stackSize = 0;  // Number of pending runs on stack
    private final int[] runBase;
    private final int[] runLen;

    private SortState(Buffer a, Comparator<? super K> c, int len) {
      this.aLength = len;
      this.a = a;
      this.c = c;

      // Allocate temp storage (which may be increased later if necessary)
      tmpLength = len < 2 * INITIAL_TMP_STORAGE_LENGTH ? len >>> 1 : INITIAL_TMP_STORAGE_LENGTH;
      tmp = s.allocate(tmpLength);

      int stackLen = (len <    120  ?  5 :
                      len <   1542  ? 10 :
                      len < 119151  ? 19 : 40);
      runBase = new int[stackLen];
      runLen = new int[stackLen];
    }

    private void pushRun(int runBase, int runLen) {
      this.runBase[stackSize] = runBase;
      this.runLen[stackSize] = runLen;
      stackSize++;
    }

    private void mergeCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if ( (n >= 1 && runLen[n-1] <= runLen[n] + runLen[n+1])
          || (n >= 2 && runLen[n-2] <= runLen[n] + runLen[n-1])) {
          if (runLen[n - 1] < runLen[n + 1])
            n--;
        } else if (runLen[n] > runLen[n + 1]) {
          break; // Invariant is established
        }
        mergeAt(n);
      }
    }

    private void mergeForceCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n - 1] < runLen[n + 1])
          n--;
        mergeAt(n);
      }
    }

    private void mergeAt(int i) {
      assert stackSize >= 2;
      assert i >= 0;
      assert i == stackSize - 2 || i == stackSize - 3;

      int base1 = runBase[i];
      int len1 = runLen[i];
      int base2 = runBase[i + 1];
      int len2 = runLen[i + 1];
      assert len1 > 0 && len2 > 0;
      assert base1 + len1 == base2;

      runLen[i] = len1 + len2;
      if (i == stackSize - 3) {
        runBase[i + 1] = runBase[i + 2];
        runLen[i + 1] = runLen[i + 2];
      }
      stackSize--;

      K key0 = s.newKey();

      int k = gallopRight(s.getKey(a, base2, key0), a, base1, len1, 0, c);
      assert k >= 0;
      base1 += k;
      len1 -= k;
      if (len1 == 0)
        return;

      len2 = gallopLeft(s.getKey(a, base1 + len1 - 1, key0), a, base2, len2, len2 - 1, c);
      assert len2 >= 0;
      if (len2 == 0)
        return;

      // Merge remaining runs, using tmp array with min(len1, len2) elements
      if (len1 <= len2)
        mergeLo(base1, len1, base2, len2);
      else
        mergeHi(base1, len1, base2, len2);
    }

    private int gallopLeft(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;
      int lastOfs = 0;
      int ofs = 1;
      K key0 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key0)) > 0) {
        // Gallop right until a[base+hint+lastOfs] < key <= a[base+hint+ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key0)) > 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        lastOfs += hint;
        ofs += hint;
      } else { // key <= a[base + hint]
        // Gallop left until a[base+hint-ofs] < key <= a[base+hint-lastOfs]
        final int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key0)) <= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key0)) > 0)
          lastOfs = m + 1;  // a[base + m] < key
        else
          ofs = m;          // key <= a[base + m]
      }
      assert lastOfs == ofs;    // so a[base + ofs - 1] < key <= a[base + ofs]
      return ofs;
    }

    private int gallopRight(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;

      int ofs = 1;
      int lastOfs = 0;
      K key1 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key1)) < 0) {
        // Gallop left until a[b+hint - ofs] <= key < a[b+hint - lastOfs]
        int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key1)) < 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      } else { // a[b + hint] <= key
        // Gallop right until a[b+hint + lastOfs] <= key < a[b+hint + ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key1)) >= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        lastOfs += hint;
        ofs += hint;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key1)) < 0)
          ofs = m;          // key < a[b + m]
        else
          lastOfs = m + 1;  // a[b + m] <= key
      }
      assert lastOfs == ofs;    // so a[b + ofs - 1] <= key < a[b + ofs]
      return ofs;
    }

    private void mergeLo(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy first run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len1);
      s.copyRange(a, base1, tmp, 0, len1);

      int cursor1 = 0;       // Indexes into tmp array
      int cursor2 = base2;   // Indexes int a
      int dest = base1;      // Indexes int a

      // Move first element of second run and deal with degenerate cases
      s.copyElement(a, cursor2++, a, dest++);
      if (--len2 == 0) {
        s.copyRange(tmp, cursor1, a, dest, len1);
        return;
      }
      if (len1 == 1) {
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); // Last elt of run 1 to end of merge
        return;
      }

      K key0 = s.newKey();
      K key1 = s.newKey();

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        /*
         * Do the straightforward thing until (if ever) one run starts
         * winning consistently.
         */
        do {
          assert len1 > 1 && len2 > 0;
          if (c.compare(s.getKey(a, cursor2, key0), s.getKey(tmp, cursor1, key1)) < 0) {
            s.copyElement(a, cursor2++, a, dest++);
            count2++;
            count1 = 0;
            if (--len2 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor1++, a, dest++);
            count1++;
            count2 = 0;
            if (--len1 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        do {
          assert len1 > 1 && len2 > 0;
          count1 = gallopRight(s.getKey(a, cursor2, key0), tmp, cursor1, len1, 0, c);
          if (count1 != 0) {
            s.copyRange(tmp, cursor1, a, dest, count1);
            dest += count1;
            cursor1 += count1;
            len1 -= count1;
            if (len1 <= 1) // len1 == 1 || len1 == 0
              break outer;
          }
          s.copyElement(a, cursor2++, a, dest++);
          if (--len2 == 0)
            break outer;

          count2 = gallopLeft(s.getKey(tmp, cursor1, key0), a, cursor2, len2, 0, c);
          if (count2 != 0) {
            s.copyRange(a, cursor2, a, dest, count2);
            dest += count2;
            cursor2 += count2;
            len2 -= count2;
            if (len2 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor1++, a, dest++);
          if (--len1 == 1)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len1 == 1) {
        assert len2 > 0;
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); //  Last elt of run 1 to end of merge
      } else if (len1 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len2 == 0;
        assert len1 > 1;
        s.copyRange(tmp, cursor1, a, dest, len1);
      }
    }

    private void mergeHi(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy second run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len2);
      s.copyRange(a, base2, tmp, 0, len2);

      int cursor1 = base1 + len1 - 1;  // Indexes into a
      int cursor2 = len2 - 1;          // Indexes into tmp array
      int dest = base2 + len2 - 1;     // Indexes into a

      K key0 = s.newKey();
      K key1 = s.newKey();

      // Move last element of first run and deal with degenerate cases
      s.copyElement(a, cursor1--, a, dest--);
      if (--len1 == 0) {
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
        return;
      }
      if (len2 == 1) {
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest);
        return;
      }

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        do {
          assert len1 > 0 && len2 > 1;
          if (c.compare(s.getKey(tmp, cursor2, key0), s.getKey(a, cursor1, key1)) < 0) {
            s.copyElement(a, cursor1--, a, dest--);
            count1++;
            count2 = 0;
            if (--len1 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor2--, a, dest--);
            count2++;
            count1 = 0;
            if (--len2 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        do {
          assert len1 > 0 && len2 > 1;
          count1 = len1 - gallopRight(s.getKey(tmp, cursor2, key0), a, base1, len1, len1 - 1, c);
          if (count1 != 0) {
            dest -= count1;
            cursor1 -= count1;
            len1 -= count1;
            s.copyRange(a, cursor1 + 1, a, dest + 1, count1);
            if (len1 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor2--, a, dest--);
          if (--len2 == 1)
            break outer;

          count2 = len2 - gallopLeft(s.getKey(a, cursor1, key0), tmp, 0, len2, len2 - 1, c);
          if (count2 != 0) {
            dest -= count2;
            cursor2 -= count2;
            len2 -= count2;
            s.copyRange(tmp, cursor2 + 1, a, dest + 1, count2);
            if (len2 <= 1)  // len2 == 1 || len2 == 0
              break outer;
          }
          s.copyElement(a, cursor1--, a, dest--);
          if (--len1 == 0)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len2 == 1) {
        assert len1 > 0;
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest); // Move first elt of run2 to front of merge
      } else if (len2 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len1 == 0;
        assert len2 > 0;
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
      }
    }

    private Buffer ensureCapacity(int minCapacity) {
      if (tmpLength < minCapacity) {
        // Compute smallest power of 2 > minCapacity
        int newSize = minCapacity;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;

        if (newSize < 0) // Not bloody likely!
          newSize = minCapacity;
        else
          newSize = Math.min(newSize, aLength >>> 1);

        tmp = s.allocate(newSize);
        tmpLength = newSize;
      }
      return tmp;
    }
  }
}

