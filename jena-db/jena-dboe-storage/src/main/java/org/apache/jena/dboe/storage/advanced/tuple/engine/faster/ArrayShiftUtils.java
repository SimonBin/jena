package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Arrays;

public class ArrayShiftUtils {
    /**
     * Shift elements between len and idx left; overrides idx
     *
     * shift ([ 0 1 2 3 4 5 ], 3, 1) -> [ 0 2 2 3 4 5 ]
     * Removes the item at index 1 (which is the item one) and shifts the next one left.
     * No further shifts are done because
     *
     *
     *
     * @param arr
     * @param len The length of the sub array starting from 0
     * @param idx
     */
    public static void takeOut(int arr[], int len, int idx) {
        for(int i = idx; i < len - 1; ++i) {
            arr[i] = arr[i + 1];
        }
    }

    public static void putBack(int arr[], int len, int idx, int value) {
        // shift right
        for(int i = len - 1; i > idx; --i) {
            arr[i] = arr[i - 1];
        }

        arr[idx] = value;
    }

    public static void main(String[] args) {
        int[] arr = new int[] { 0, 1, 2, 3, 4, 5};

        int v1 = arr[2];
        takeOut(arr, arr.length, 2);
        System.out.println(Arrays.toString(arr));

        int v2 = arr[5];
        takeOut(arr, arr.length - 1, 5);
        System.out.println(Arrays.toString(arr));

        int v3 = arr[0];
        takeOut(arr, arr.length - 2, 0);
        System.out.println(Arrays.toString(arr));


        putBack(arr, arr.length - 2, 0, v3);
        System.out.println(Arrays.toString(arr));

        putBack(arr, arr.length - 1, 5, v2);
        System.out.println(Arrays.toString(arr));

        putBack(arr, arr.length, 2, v1);
        System.out.println(Arrays.toString(arr));
    }

}
