#include <gtest/gtest.h>

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"

TEST(FSTest, Basics) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager* bpm = new dbs::fs::BufPageManager(fm);
    EXPECT_TRUE(fm->createFile("testfile.txt"));
    EXPECT_TRUE(fm->createFile("testfile2.txt"));
    int fileID, f2;
    fileID = fm->openFile("testfile.txt");
    f2 = fm->openFile("testfile2.txt");
    ASSERT_NE(fileID, -1);
    ASSERT_NE(f2, -1);
    for (int pageID = 0; pageID < 12000; pageID++) {
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        ASSERT_NE(b, nullptr);
        // 注意，在allocPage或者getPage后，千万不要进行delete[] b这样的操作
        // 内存的分配和管理都在BufPageManager中做好，不需要关心，如果自行释放会导致问题
        b[0] = pageID;  // 对缓存页进行写操作
        b[1] = fileID;
        bpm->markDirty(index);  // 标记脏页
        // 在重新调用allocPage获取另一个页的数据时并没有将原先b指向的内存释放掉
        // 因为内存管理都在BufPageManager中做好了
        b = bpm->getPage(f2, pageID, index);
        ASSERT_NE(b, nullptr);
        b[0] = pageID;
        b[1] = f2;
        bpm->markDirty(index);
    }
    for (int pageID = 0; pageID < 12000; pageID++) {
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        ASSERT_EQ(b[0], pageID);
        ASSERT_EQ(b[1], fileID);
        bpm->access(index);  // 标记访问
        b = bpm->getPage(f2, pageID, index);
        ASSERT_EQ(b[0], pageID);
        ASSERT_EQ(b[1], f2);
        bpm->access(index);
    }
    for (int pageID = 9999; pageID >= 0; pageID--) {
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        ASSERT_EQ(b[0], pageID);
        ASSERT_EQ(b[1], fileID);
        bpm->access(index);  // 标记访问
        b = bpm->getPage(f2, pageID, index);
        ASSERT_EQ(b[0], pageID);
        ASSERT_EQ(b[1], f2);
        bpm->access(index);
    }

    for (int pageID = 12000; pageID < 12100; pageID++) {
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        ASSERT_NE(b, nullptr);
        // 注意，在allocPage或者getPage后，千万不要进行delete[] b这样的操作
        // 内存的分配和管理都在BufPageManager中做好，不需要关心，如果自行释放会导致问题
        for (int i = 0; i < 10; i++) b[i] = i + pageID;
        bpm->markDirty(index);  // 标记脏页
        // 在重新调用allocPage获取另一个页的数据时并没有将原先b指向的内存释放掉
        // 因为内存管理都在BufPageManager中做好了
        b = bpm->getPage(f2, pageID, index);
        ASSERT_NE(b, nullptr);
        for (int i = 0; i < 10; i++) b[i] = i + pageID;
        bpm->markDirty(index);
    }
    bpm->close();
    fm->closeFile(fileID);
    fm->closeFile(f2);
    delete bpm;
    delete fm;

    fm = new dbs::fs::FileManager();
    bpm = new dbs::fs::BufPageManager(fm);
    fileID = fm->openFile("testfile.txt");
    f2 = fm->openFile("testfile2.txt");

    for (int pageID = 12000; pageID < 12100; pageID++) {
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        ASSERT_NE(b, nullptr);
        // 注意，在allocPage或者getPage后，千万不要进行delete[] b这样的操作
        // 内存的分配和管理都在BufPageManager中做好，不需要关心，如果自行释放会导致问题
        for (int i = 0; i < 10; i++) ASSERT_EQ(b[i], i + pageID);
        bpm->access(index);  // 标记访问
        // 在重新调用allocPage获取另一个页的数据时并没有将原先b指向的内存释放掉
        // 因为内存管理都在BufPageManager中做好了
        b = bpm->getPage(f2, pageID, index);
        ASSERT_NE(b, nullptr);
        for (int i = 0; i < 10; i++) ASSERT_EQ(b[i], i + pageID);
        bpm->access(index);  // 标记访问
    }
    bpm->close();
    fm->closeFile(fileID);
    fm->closeFile(f2);
    delete bpm;
    delete fm;
}

TEST(FSTest, OpenFileFail) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    ASSERT_EQ(fm->openFile("NotExist.txt"), -1);
}

TEST(FSTest, CreateFileFail) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    EXPECT_FALSE(fm->createFile("lib"));
}

TEST(FSTest, DeleteFileFail) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    EXPECT_FALSE(fm->deleteFile("lib"));
}

TEST(FSTest, CreateFolder) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    EXPECT_TRUE(fm->createFolder("testfolder"));
    EXPECT_FALSE(fm->createFolder("testfolder"));
    EXPECT_TRUE(fm->deleteFolder("testfolder"));
    EXPECT_FALSE(fm->deleteFolder("testfolder"));
}

TEST(FSTest, ExistFile) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    EXPECT_TRUE(fm->existFile("Makefile"));
    EXPECT_FALSE(fm->existFile("NotExist.txt"));
}
