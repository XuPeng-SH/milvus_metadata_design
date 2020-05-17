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

    collection_ids = sss.GetCollectionIds();
    for (auto id : collection_ids) {
        std::cout << "CID=" << id << " CNAME=" << sss.GetSnapshot(id)->GetName() << std::endl;
    }

    SegmentFileContext sf_context;
    sf_context.field_name = "f_1_1";
    sf_context.field_element_name = "fe_1_1";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    OperationContext context;
    BuildOperation build_op(context, 1);
    auto seg_file = build_op.NewSegmentFile(sf_context);
    build_op.OnExecute();

    auto prev_ss = build_op.GetPrevSnapshot();
    OperationContext n_seg_context;
    n_seg_context.prev_partition = prev_ss->GetPartition(1);
    NewSegmentOperation n_seg_op(n_seg_context, prev_ss);
    n_seg_op.NewSegment();
    n_seg_op.NewSegmentFile(sf_context);
    n_seg_op.OnExecute();

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
