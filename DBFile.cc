#include <iostream>
#include <cstdlib>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile (): mPageIndex(0), mIsWriteBufferDirty(false){
    mFile = new File();
    mWriteBuffer = new Page();
    mReadBuffer = new Page();
    mCurrentRecord = new Record();
}

// Destructor
DBFile::~DBFile () {
    delete mCurrentRecord;
    delete mReadBuffer;
    delete mWriteBuffer;
    delete mFile;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    int lResult = 1;
    switch(f_type){
    case heap:
        {
            cout<<"creating heap file..."<<endl;
            mFile->Open(0, f_path);
            cout<<"created heap file"<<endl;
            break;
        }
    case sorted:
        {
            cout<<"File is of type Sorted"<<endl;
            break;
        }
    case tree:
        {
            cout<<"File is of type Tree"<<endl;
            break;
        }
    default:
        cout<<"File type not valid!"<<endl;
        lResult = 0;
    }
    mFile->Close();
    return lResult;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    FILE* loadFile = fopen(loadpath, "r");
    if(loadFile == NULL){
        cerr<<"Error in opening the file: "<<loadpath<<endl;
        exit(1);
    }
    Record lTempRecord;
    while(lTempRecord.SuckNextRecord(&f_schema, loadFile)){
        Add(lTempRecord);
    }
    fclose(loadFile);
}

int DBFile::Open (char *f_path) {
    if(f_path == NULL) {
        return 0;
    }
    cout<<"Opening the file "<<f_path<<endl;
    mFile->Open(1, f_path);
    // read buffer points to first of file
    mFile->GetPage(mReadBuffer, 0);
    // write buffer points to last of file
    mFile->GetPage(mWriteBuffer, mFile->GetLength());
    return 1;
}

void DBFile::MoveFirst () {
    // Get the first page and put it in mWriteBuffer
    mFile->GetPage (mReadBuffer, 1);
    // Get the first record and put it in mCurrentRecord
    mReadBuffer->GetFirst(mCurrentRecord);
    mPageIndex = 0;
}

int DBFile::Close () {
    mFile->Close();
}

void DBFile::Add (Record &rec) {
    Record lTemp;
    lTemp.Consume(&rec);
    int lAddResult = mWriteBuffer->Append(&lTemp);
    if(!lAddResult){
        cout<<"Record cannot fit into current page.."<<endl;
        cout<<"Writing to the disk"<<endl;
        mFile->AddPage(mWriteBuffer, mFile->GetLength());
        // Empty the page out
        mWriteBuffer->EmptyItOut();
        // add the record to the buffer
        mWriteBuffer->Append(&lTemp);
    }
    mIsWriteBufferDirty = 1;
}

int DBFile::GetNext (Record &fetchme) {

    if(mReadBuffer->GetFirst(&fetchme) == 0){ // if no records present in read buffer
        mPageIndex++;
        // read next page
        mReadBuffer->EmptyItOut();
        mFile->GetPage(mReadBuffer, mPageIndex);
        // get the next record from the read buffer
        mReadBuffer->GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
