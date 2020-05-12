#include <iostream>
#include <thread>
#include <unistd.h>
#include <memory>
#include "Utils.h"
#include "Resources.h"
#include "Snapshots.h"
#include "Proxy.h"

using namespace std;

int main() {
    /* { */
        /* auto ss_holder = make_shared<SnapshotsHolder>(1); */
        /* ss_holder->SetGCHandler(std::bind(&SnapshotsHolder::GCHandlerTestCallBack, ss_holder, std::placeholders::_1)); */
        /* thread gc_thread(&SnapshotsHolder::BackgroundGC, ss_holder); */
        /* ss_holder->Add(1); */
        /* ss_holder->Add(2); */
        /* ss_holder->Add(3); */

        /* { */
        /*     auto ss = ss_holder->GetSnapshot(3); */
        /*     if (!ss) cout << 3 << " ss is nullptr" << endl; */
        /*     else { */
        /*         cout << "3 ss refcnt = " << ss->RefCnt() << endl; */
        /*     } */
        /* } */
        /* { */
        /*     auto ss = ss_holder->GetSnapshot(3, false); */
        /*     if (!ss) cout << 3 << " ss is nullptr" << endl; */
        /*     else { */
        /*         cout << "3 ss refcnt = " << ss->RefCnt() << endl; */
        /*     } */
        /* } */

        /* ss_holder->NotifyDone(); */
        /* gc_thread.join(); */
    /* } */

    auto& sss = Snapshots::GetInstance();
    auto ss_holder = sss.GetHolder(1);

    auto collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        cout << "SSS cid=" << id << endl;
    }

    sss.Close(2);
    collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        cout << "SSS2 cid=" << id << endl;
    }

    return 0;
}
