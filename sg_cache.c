////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : YOUR NAME
//   Last Modified : 
//

// Include Files
#include <stdlib.h>
#include <cmpsc311_log.h>
#include <string.h>
#include <sg_defs.h>
// Project Includes
#include <sg_cache.h>

// Defines

// Functional Prototypes

// Global Variables

SG_CACHE* caches;
SG_HITMISS hitMiss;
int cacheSize = 0;
int time1 = 0;
int maxCacheSize;
//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) {
    // Return successfully
    caches = realloc(caches, maxElements*sizeof(SG_CACHE));
    maxCacheSize = maxElements;
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) {
    printf("The total number of hits is %ld and the total number of misses is %ld, and the hit/miss ratio is %f",
     hitMiss.SG_HITS, hitMiss.SG_MISSES, (float)hitMiss.SG_HITS/(hitMiss.SG_MISSES +hitMiss.SG_HITS));
    
    free(caches);
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) {
    for(int x = 0; x<cacheSize; x++){
        if(nde == caches[x].CNODE && blk == caches[x].CBLOCK){
            return(caches[x].CDATA);
        }
    }
    return( NULL );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Insert the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) {
    int LRU = caches[0].CTIME;
    int index = 0;
    //iterate through cache - results in finding index of lowest LRU time
    for(int x = 0; x<cacheSize; x++){
        if(nde == caches[x].CNODE && blk == caches[x].CBLOCK){
            //if cache block needs to be updated
            if(memcmp(caches[x].CDATA, block, 1024) != 0){
                memcpy(caches[x].CDATA, block, 1024);           
            }
            return(0);
        }
        if(caches[x].CTIME<LRU){
            LRU = caches[x].CTIME;
            index = x;
        }
    }
    //if cache still has empty spots
    if(cacheSize<maxCacheSize){
        caches[cacheSize].CBLOCK = blk;
        caches[cacheSize].CNODE = nde;
        memcpy(caches[cacheSize].CDATA, block, 1024);
        caches[cacheSize].CTIME = time1;
        time1++;
        cacheSize++;
    }else{
    //if cache is full
        caches[index].CBLOCK = blk;
        caches[index].CNODE = nde;
        memcpy(caches[index].CDATA, block, 1024);
        caches[index].CTIME = time1;
        time1++;
    }
    return(-1);
}
