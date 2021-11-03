////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : 
//   Last Modified : 
//

// Include Files

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>
#include <string.h>
#include <sg_cache.h>
#include <stdlib.h>
#include <math.h>
// Defines


// Driver file entry

// Global data
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno;      // The local sequence number

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint


int maxSequences = 1;
int totalSequences = 0;
SG_SEQ* sequences;

int totalFiles = 0;
int maxFiles = 1;
SG_FILE* files;

extern SG_HITMISS hitMiss;
//
// Functions

//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgcheck
// Description  : Check if file is open
//
// Inputs       : handle - handle of file
// Outputs      : file position if open, -1 if failure
int sgcheck(SgFHandle fh){
    //check if file is open
    for(int x =0; x<totalFiles; x++){
        if(files[x].SG_FILE_HANDLE == fh && files[x].SG_FILE_STATUS == 1){
            return(x);
        }
    }
    return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgfilesizecheck
// Description  : Check if maximum files is reached
//
// Inputs       : N/A
// Outputs      : 0 if succesful, -1 if not
int sgfilesizecheck(){
    
    //check if number of files = maximum files allowed
    if(totalFiles == maxFiles){
        files = realloc(files, maxFiles*2*sizeof(SG_FILE));
        if(files == NULL){
            free(files);
            logMessage( LOG_ERROR_LEVEL, "Could not reallocate memory for files" );
            return(-2);
        }

        maxFiles*=2;
        return(0);
    }
    
    return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgsequencesizecheck
// Description  : Check if maximum sequences is reached
//
// Inputs       : N/A
// Outputs      : 0 if succesful, -1 if not
int sgsequencesizecheck(){
    
    //check if number of sequences = maximum sequences allowed
    if(totalSequences == maxSequences){
        sequences = realloc(sequences, maxSequences*2*sizeof(SG_SEQ));
        if(sequences == NULL){
            free(sequences);
            logMessage( LOG_ERROR_LEVEL, "Could not reallocate memory for sequences" );
            return(-2);
        }
        maxSequences*=2;
        return(0);
    }
    
    return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgblocksizecheck
// Description  : Check if maximum blocks is reached
//
// Inputs       : N/A
// Outputs      : 0 if succesful, -1 if not
int sgblocksizecheck(SgFHandle fh){
    
    //check if number of files = maximum files allowed
    if(files[fh].SG_FILE_POINTER == files[fh].maxBlocks){
        files[fh].SG_FILE_BLOCKS = realloc(files[fh].SG_FILE_BLOCKS, files[fh].maxBlocks*2*sizeof(SG_Block_ID));
        if(files[fh].SG_FILE_BLOCKS == NULL){
            free(files[fh].SG_FILE_BLOCKS);
            logMessage( LOG_ERROR_LEVEL, "Could not reallocate memory for block IDs" );
            return(-2);
        }

        files[fh].SG_FILE_NODES = realloc(files[fh].SG_FILE_NODES, files[fh].maxBlocks*2*sizeof(SG_Node_ID));
        if(files[fh].SG_FILE_NODES == NULL){
            free(files[fh].SG_FILE_NODES);
            logMessage( LOG_ERROR_LEVEL, "Could not reallocate memory for node IDs" );
            return(-2);
        }

        files[fh].maxBlocks*=2;
        return(0);
    }
    
    return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgsequencecheck
// Description  : Check for and update sequences
//
// Inputs       : srem, rem
// Outputs      : 0 if update, -1 if created
int sgsequencecheck(SG_SeqNum srem, SG_Node_ID rem){
        for(int x = 0; x<totalSequences; x++){
            if(sequences[x].NID == rem){
                sequences[x].SNum = srem;
                return(0);
            }
        }
        sequences[totalSequences].NID = rem;
        sequences[totalSequences].SNum = srem;
        totalSequences++;
        if(sgsequencesizecheck() == -2){
            return(-2);
        }
        return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sggetsequence
// Description  : Returns the sequence number of given node ID if found
//
// Inputs       : node ID
// Outputs      : seq number if found, -1 if not found
int sggetsequence(SG_Node_ID rem){
        for(int x = 0; x<totalSequences; x++){
            if(sequences[x].NID == rem){
                return(++sequences[x].SNum);
            }
        }
        return(-1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {

    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( -1 );
        }

        // Set to initialized
        sgDriverInitialized = 1;
    }
    if(sgfilesizecheck() == -2){
        return(-1);
    }

    //file struct assignment
    SG_FILE file = {0};
    file.SG_FILE_HANDLE = totalFiles;

    file.SG_FILE_PATHNAME = malloc(strlen(path));
    if(file.SG_FILE_PATHNAME == NULL){
        free(file.SG_FILE_PATHNAME);
        return(-1);
    }
    strcpy(file.SG_FILE_PATHNAME, path);

    file.SG_FILE_STATUS = 1;

    file.SG_FILE_NODES = calloc(1, sizeof(SG_Node_ID));
    if(file.SG_FILE_NODES == NULL){
        free(file.SG_FILE_NODES);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for node IDs" );
        return(-1);
    }

    file.SG_FILE_BLOCKS = calloc(1, sizeof(SG_Block_ID));
    if(file.SG_FILE_BLOCKS == NULL){
        free(file.SG_FILE_BLOCKS);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for block IDs" );
        return(-1);
    }

    file.maxBlocks = 1;

    memcpy(&files[totalFiles], &file, sizeof(SG_FILE));

    totalFiles++;
    
    // Return the file handle 
    return( file.SG_FILE_HANDLE );

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {

    //Local variables
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_DATA_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc = sgLocalNodeId, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    char * tempBuf = (char *) malloc(1024);
    if(tempBuf == NULL){
        free(tempBuf);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for read buffer" );
        return(-1);
    }
    //file handle check
    int x = sgcheck(fh);
    if(x== -1){
        return(-1);
    }

    //pointer check
    if(files[x].SG_FILE_POINTER+(len/1024.0)>files[x].SG_FILE_SIZE){
        return(-1);
    }


    //check cache first baby
    //check if data in cache
    if(getSGDataBlock(files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER], 
    files[x].SG_FILE_BLOCKS[(int)files[x].SG_FILE_POINTER])){
        //copy the entire block into tempBuf
        memcpy(tempBuf, getSGDataBlock(files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER], 
        files[x].SG_FILE_BLOCKS[(int)files[x].SG_FILE_POINTER]), 1024);
        //update hits
        hitMiss.SG_HITS++;
    }else{ 
        //update misses
        hitMiss.SG_MISSES++;
        //Get ready FOR SCATTERGATHERRRRRR
        //Setup the packet
        pktlen = SG_BASE_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                        files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER],   // Remote ID
                                        files[x].SG_FILE_BLOCKS[(int)files[x].SG_FILE_POINTER],  // Block ID
                                        SG_OBTAIN_BLOCK,  // Operation
                                        sgLocalSeqno++,    // Sender sequence number
                                        sggetsequence(files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER]),  // Receiver sequence number
                                        NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgObtainBlock: failed serialization of packet [%d].", ret );
            return( -1 );
        }
        
        //Send the packet
        rpktlen = SG_DATA_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgObtainBlock: failed packet post" );
            return( -1 );
        }

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, tempBuf, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgObtainBlock: failed deserialization of packet [%d]", ret );
            return( -1 );
        }
        
    }

    //Remember tempBuf contains the ENTIRE BLOCK
    // Decide on what parts of tempBuf are copied to buf
    
    //formula converts pointer to index in tempBuf
    int convertedIndex = (int)((files[x].SG_FILE_POINTER-floor(files[x].SG_FILE_POINTER))*1024);
    memcpy(buf, &tempBuf[convertedIndex], len);
    

    //update pointer
    files[x].SG_FILE_POINTER+=(len/(float)1024);
    free(tempBuf);
    tempBuf = NULL;

    // Sanity check the return value
    if ( loc != sgLocalNodeId ) {
        logMessage( LOG_ERROR_LEVEL, "sgObtainBlock: bad local ID returned [%ul]", loc );
        return( -1 );
    }  
    // Return the bytes processed
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {

    //Local variables
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc = sgLocalNodeId, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    char * tempBuf = (char *) calloc(1024, sizeof(char));
    if(tempBuf == NULL){
        free(tempBuf);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for write buffer" );
        return(-1);
    }
    //file handle check
    int x = sgcheck(fh);
    if(x== -1){
        return(-1);
    }

    pktlen = SG_DATA_PACKET_SIZE;


    //if pointer is not at the end or not at a boundary
    if(ceilf(files[x].SG_FILE_POINTER) != files[x].SG_FILE_POINTER || files[x].SG_FILE_POINTER != (float)files[x].SG_FILE_SIZE){
        //we will read the data into tempBuf of the block we are working on

        //store file pointer before read operation
        double storedPointer = files[x].SG_FILE_POINTER;
        files[x].SG_FILE_POINTER= floor(files[x].SG_FILE_POINTER);
        sgread(fh, tempBuf, 1024);
        files[x].SG_FILE_POINTER = storedPointer;
    }


    //formula converts pointer to index in tempBuf
    int convertedIndex = (int)((files[x].SG_FILE_POINTER-floor(files[x].SG_FILE_POINTER))*1024);        

    //overwrites the previous block with updated data from buf into tempBuf
    memcpy(&tempBuf[convertedIndex], buf, len);

    //if the data pointer is before the end of the data (use update block)
    if(files[x].SG_FILE_POINTER<files[x].SG_FILE_SIZE){
        
        
        
        //Setup the packet
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER],   // Remote ID
                                    files[x].SG_FILE_BLOCKS[(int)files[x].SG_FILE_POINTER],  // Block ID
                                    SG_UPDATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    sggetsequence(files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER]),  // Receiver sequence number
                                    tempBuf, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgUpdateBlock: failed serialization of packet [%d].", ret );
            return( -1 );
        }
    
        //Send the packet
        rpktlen = SG_BASE_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgUpdateBlock: failed packet post" );
            return( -1 );
        }

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgUpdateBlock: failed deserialization of packet [%d]", ret );
            return( -1 );
        }

        // Sanity check the return value
        if ( loc != sgLocalNodeId ) {
            logMessage( LOG_ERROR_LEVEL, "sgUpdateBlock: bad local ID returned [%ul]", loc );
            return( -1 );
        }
          

    //if the data pointer is after the end of the packet 
    }else if(files[x].SG_FILE_POINTER>files[x].SG_FILE_SIZE){
            return(-1);
    }else{
    //if the pointer is at the end of the packet (use create block)

        //check block size
        if(sgblocksizecheck(x) == -2){
            return(-1);
        }


        //Setup the packet
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_CREATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    tempBuf, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: failed serialization of packet [%d].", ret );
            return( -1 );
            }
    
    
        //Send the packet
        rpktlen = SG_BASE_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: failed packet post" );
            return( -1 );
        }

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: failed deserialization of packet [%d]", ret );
            return( -1 );
        }

        // Sanity check the return value
        if ( loc != sgLocalNodeId ) {
            logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: bad local ID returned [%ul]", loc );
            return( -1 );
        }
        if(sgsequencecheck(srem, rem) == -2){
            return(-1);
        }
    }

    //update block and node IDs
    files[x].SG_FILE_BLOCKS[(int)files[x].SG_FILE_POINTER] = blkid;
    files[x].SG_FILE_NODES[(int)files[x].SG_FILE_POINTER] = rem;

    //update pointer and size
    files[x].SG_FILE_POINTER+=(len/(float)1024);
    if(op == SG_CREATE_BLOCK){
        files[x].SG_FILE_SIZE++;
    }

    //update the cache
    if(putSGDataBlock(rem, blkid, tempBuf) == 0){
        hitMiss.SG_HITS++;
    }else{
        hitMiss.SG_MISSES++;
    }
    free(tempBuf);
    tempBuf = NULL;


    // Log the write, return bytes written
    return( len );

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {

    //file handle check
    int x = sgcheck(fh);
    if(x== -1 || off/1024.0 >files[x].SG_FILE_SIZE){
        return(-1);
    }
    files[x].SG_FILE_POINTER = off/(float)1024;

    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {

    //file handle check
    int x = sgcheck(fh);
    if(x== -1){
        return(-1);
    }
    files[x].SG_FILE_STATUS = 0;
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

    //Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Packet_Status ret;

    //close all open files, and FREE EVERYTHING
    for(int x = 0;x<totalFiles; x++){
        sgclose(x);
        free(files[x].SG_FILE_PATHNAME);
        free(files[x].SG_FILE_NODES);
        free(files[x].SG_FILE_BLOCKS);
    }

    //FREE EVERYTHING CONTINUED
    free(files);
    free(sequences);

    //Setup the packet
    if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_STOP_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: failed serialization of packet [%d].", ret );
        return( -1 );
    }
    
    //Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgCreateBlock: failed packet post" );
        return( -1 );
    }
    //close cache
    closeSGCache();
    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );


    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

    // Return the system function return value
            char ind;      
            if(loc==0){
                return 1;
            }else if(rem==0){
                return 2;
            }else if(blk==0){
                return 3;
            }else if(op<0||op>6){
                return 4;
            }else if(sseq==0){
                return 5;
            }else if(rseq==0){
                return 6;
            } else if (packet == NULL){
                return 9;
            }
            uint32_t magic = SG_MAGIC_VALUE;
            memcpy(packet, &magic, 4);
            memcpy(&packet[4], &loc, 8);
            memcpy(&packet[12], &rem, 8);
            memcpy(&packet[20], &blk, 8);
            memcpy(&packet[28], &op, 4);
            memcpy(&packet[32], &sseq, 2);
            memcpy(&packet[34], &rseq, 2);
            //check if data is null
            if(data == NULL){
                ind = 0;
                memcpy(&packet[36], &ind, 1);
                *plen = SG_BASE_PACKET_SIZE;
            }else{
                ind = 1;
                memcpy(&packet[36], &ind, 1);
                memcpy(&packet[37], data, SG_BLOCK_SIZE);
                *plen = SG_DATA_PACKET_SIZE;
            }
            memcpy(&packet[37+(int)ind*SG_BLOCK_SIZE], &magic, 4);

            return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

    // Return the system function return value
            if(packet==NULL){
                return 9;
            }
            char *ptr1 = &packet[4];

            //check local ID
            uint64_t *ptr = (uint64_t *) ptr1;
            if(*ptr == 0){
                return 1;
            }
            *loc = *ptr;
            ptr +=1;

            //check remote ID
            if(*ptr == 0){
                return 2;
            }
            *rem = *ptr;
            ptr+=1;

            //check block ID
            if(*ptr == 0){
                return 3;
            }
            *blk = *ptr;
            ptr +=1;

            //check op
            uint32_t *op_ptr = (uint32_t *) ptr;
            *op = *op_ptr;
            if(*op<0||*op>6){
                return 4;
            }
            op_ptr+=1;

            //check sender
            int16_t *seq_ptr = (int16_t *) op_ptr;
            if(*seq_ptr == 0){
                return 5;
            }
            *sseq = *seq_ptr;
            seq_ptr +=1;

            //check receiver
            if(*seq_ptr == 0){
                return 6;
            }
            *rseq = *seq_ptr;

            seq_ptr+=1;

            //check value of data indicator
            ptr1+=32;
            if(*ptr1 == 0){
                if(plen != SG_BASE_PACKET_SIZE){
                    return 8;
                }else{
                    data = NULL;
                }
            }else{
                if(data==NULL){
                    return 7;
                }
                if(plen != SG_DATA_PACKET_SIZE){
                    return 8;
                }else{
                    ptr1+=1;
                    memcpy(data, ptr1, SG_BLOCK_SIZE);
                }
            }
            return 0;
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    
    //initiate the cache
    uint16_t maxElems = SG_MAX_CACHE_ELEMENTS;
    initSGCache(maxElems); 
    
    //Initiate files
    files = malloc(sizeof(SG_FILE));
    if(files == NULL){
        free(files);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for files" );
        return(-1);
    }

    //Initiate sequences
    sequences = malloc(sizeof(SG_SEQ));
    if(sequences == NULL){
        free(sequences);
        logMessage( LOG_ERROR_LEVEL, "Could not allocate memory for sequences" );
        return(-1);
    }


    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;
    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );
    return( 0 );
}
