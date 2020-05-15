#include <iostream>
#include <thread>
#include <unistd.h>
#include <memory>
#include "Utils.h"
#include "Resources.h"
#include "Snapshots.h"
#include "Proxy.h"
#include "schema.pb.h"
#include "Operations.h"

using namespace std;

const int32_t IVFSQ8 = 1;

schema::CollectionSchemaPB proto_lab() {
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
    ivf_sq8_element_info->set_type(IVFSQ8);
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
    collection_schema.mutable_fields()->Add(std::move(vector_field));

    return collection_schema;
}

int main() {
    /* { */
    /*     auto ss_holder = make_shared<SnapshotsHolder>(1); */
    /*     ss_holder->SetGCHandler(std::bind(&SnapshotsHolder::GCHandlerTestCallBack, ss_holder, std::placeholders::_1)); */
    /*     thread gc_thread(&SnapshotsHolder::BackgroundGC, ss_holder); */
    /*     ss_holder->Add(1); */
    /*     ss_holder->Add(2); */
    /*     ss_holder->Add(3); */

    /*     { */
    /*         auto ss = ss_holder->GetSnapshot(3); */
    /*         if (!ss) cout << 3 << " ss is nullptr" << endl; */
    /*         else { */
    /*             cout << "3 ss refcnt = " << ss->RefCnt() << endl; */
    /*         } */
    /*     } */
    /*     { */
    /*         auto ss = ss_holder->GetSnapshot(3, false); */
    /*         if (!ss) cout << 3 << " ss is nullptr" << endl; */
    /*         else { */
    /*             cout << "3 ss refcnt = " << ss->RefCnt() << endl; */
    /*         } */
    /*     } */

    /*     ss_holder->NotifyDone(); */
    /*     gc_thread.join(); */
    /* } */
    /* return 0; */

    Store::GetInstance().Mock();
    auto& sss = Snapshots::GetInstance();
    auto ss_holder = sss.GetHolder("c_1");
    if (ss_holder)
        cout << "ss_holder id=" << ss_holder->GetID() << " name=" << ss_holder->GetSnapshot()->GetName() << std::endl;

    auto collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        cout << "SSS cid=" << id << endl;
    }

    sss.Close(2);

    /* auto collection_schema = proto_lab(); */
    /* cout << collection_schema.fields_size() << endl; */
    /* cout << collection_schema.fields(0).name() << endl; */
    /* auto c = Store::GetInstance().CreateCollection(collection_schema); */
    /* auto holder = sss.GetHolder(c->GetID()); */

    /* cout << element.info().params_size() << endl; */
    collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        std::cout << "CID=" << id << " CNAME=" << sss.GetSnapshot(id)->GetName() << std::endl;
    }

    /* holder->GetSnapshot()->GetPartitionNames(); */
    /* Collection ccc("CCC"); */
    /* ccc.SetID(111); */
    /* Segment seg(1); */
    /* seg.SetID(222); */
    /* Store::GetInstance().DoCommit(ccc, seg); */

    /* using ResourcesT = std::tuple<CollectionCommitMap, SchemaCommitMap>; */
    /* cout << "XXX " << Index<CollectionCommit, ResourcesT>::value << endl; */

    BuildContext context;
    context.field_name = "f_1_1";
    context.field_element_name = "fe_1_1";
    context.segment_id = 1;
    context.partition_id = 1;
    BuildOperation b1(context, 1);
    auto prev_ss = b1.GetPrevSnapshot();
    cout << "Prev b1 SS " << prev_ss->GetName() << " RefCnt=" << prev_ss->RefCnt() << endl;
    for(auto name : prev_ss->GetFieldNames()) {
        std::cout << "FieldName: " << name << std::endl;
    }

    /* BuildOperation build(prev_ss, context); */
    /* cout << "Prev Build SS " << build.GetPrevSnapshot()->GetName() << " RefCnt=" << build.GetPrevSnapshot()->RefCnt() << endl; */

    NewSegmentFileOperation new_sf_op(prev_ss, context);
    new_sf_op.OnExecute();

    auto segment_file = new_sf_op.GetSegmentFile();
    b1.AddStep(*segment_file);
    b1.OnExecute();


    /* build.OnExecute(); */

    /* for(auto id : prev_ss->GetSegmentIds()) { */
    /*     std::cout << "Segment id=" << id << std::endl; */
    /* } */

    /* for(auto id : prev_ss->GetSegmentFileIds()) { */
    /*     std::cout << "SegmentFile id=" << id << std::endl; */
    /* } */

    /* for(auto name : prev_ss->GetFieldElementNames()) { */
    /*     std::cout << "FieldElement name=" << name << std::endl; */
    /* } */

    /* for(auto name : prev_ss->GetFieldNames()) { */
    /*     std::cout << "Field name=" << name << std::endl; */
    /* } */

    /* for(auto id : prev_ss->GetPartitionIds()) { */
    /*     std::cout << "Partition id=" << id << std::endl; */
    /* } */


    return 0;
}
