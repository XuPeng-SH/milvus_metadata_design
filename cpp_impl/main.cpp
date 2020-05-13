#include <iostream>
#include <thread>
#include <unistd.h>
#include <memory>
#include "Utils.h"
#include "Resources.h"
#include "Snapshots.h"
#include "Proxy.h"
#include "schema.pb.h"

using namespace std;

schema::FieldSchemaPB proto_lab() {
    schema::CollectionSchemaPB collection_schema;
    collection_schema.set_name("new_c");
    schema::FieldSchemaPB vector_field;
    vector_field.set_name("vector");
    schema::FieldInfoPB* vector_field_info = new schema::FieldInfoPB;
    vector_field_info->set_type(schema::VECTOR);
    schema::ParamPB dimension_param;
    dimension_param.set_key("dimension");
    dimension_param.set_value("512");
    vector_field_info->mutable_params()->Add(std::move(dimension_param));

    schema::FieldElementSchemaPB ivf_sq8_element;
    ivf_sq8_element.set_name("IVFSQ8");
    schema::FieldElementInfoPB* ivf_sq8_element_info = new schema::FieldElementInfoPB;
    ivf_sq8_element_info->set_type("IVFSQ8");
    schema::ParamPB nprobe_param;
    nprobe_param.set_key("nprobe");
    nprobe_param.set_value("32");
    schema::ParamPB metric_param;
    nprobe_param.set_key("metric_type");
    nprobe_param.set_value("IP");

    ivf_sq8_element_info->mutable_params()->Add(std::move(nprobe_param));
    ivf_sq8_element_info->mutable_params()->Add(std::move(metric_param));
    ivf_sq8_element.set_allocated_info(ivf_sq8_element_info);

    vector_field.mutable_elements()->Add(std::move(ivf_sq8_element));
    vector_field.set_allocated_info(vector_field_info);

    return vector_field;
}

int main() {
    auto field = proto_lab();
    cout << field.elements_size() << endl;
    cout << field.elements(0).name() << endl;
    /* cout << element.info().params_size() << endl; */
    return 0;
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
    auto ss_holder = sss.GetHolder("c_1");
    cout << "ss_holder id=" << ss_holder->GetID() << " name=" << ss_holder->GetSnapshot()->GetName() << std::endl;

    auto collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        cout << "SSS cid=" << id << endl;
    }

    sss.Close(2);
    collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        std::cout << "CID=" << id << " CNAME=" << sss.GetSnapshot(id)->GetName() << std::endl;
    }

    return 0;
}
