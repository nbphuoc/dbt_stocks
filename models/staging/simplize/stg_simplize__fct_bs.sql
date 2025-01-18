with
    source as (select * from {{ source("simplize", "fct_bs") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            quarter as fk_quarter_id,
            coalesce(`tài_sản_ngắn_hạn`, 0) as tai_san_ngan_han,
            coalesce(
                `tiền_và_các_khoản_tương_đương_tiền`, 0
            ) as tien_va_cac_khoan_tuong_duong_tien,
            coalesce(`tiền`, 0) as tien,
            coalesce(`các_khoản_tương_đương_tiền`, 0) as cac_khoan_tuong_duong_tien,
            coalesce(`đầu_tư_tài_chính_ngắn_hạn`, 0) as dau_tu_tai_chinh_ngan_han,
            coalesce(`chứng_khoán_kinh_doanh`, 0) as chung_khoan_kinh_doanh,
            coalesce(
                `dự_phòng_giảm_giá_chứng_khoán_kinh_doanh`, 0
            ) as du_phong_giam_gia_chung_khoan_kinh_doanh,
            coalesce(
                `đầu_tư_nắm_giữ_đến_ngày_đáo_hạn`, 0
            ) as dau_tu_nam_giu_den_ngay_dao_han,
            coalesce(`các_khoản_phải_thu_ngắn_hạn`, 0) as cac_khoan_phai_thu_ngan_han,
            coalesce(
                `phải_thu_ngắn_hạn_của_khách_hàng`, 0
            ) as phai_thu_ngan_han_cua_khach_hang,
            coalesce(
                `trả_trước_cho_người_bán_ngắn_hạn`, 0
            ) as tra_truoc_cho_nguoi_ban_ngan_han,
            coalesce(`phải_thu_nội_bộ_ngắn_hạn`, 0) as phai_thu_noi_bo_ngan_han,
            coalesce(
                `phải_thu_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng`, 0
            ) as phai_thu_theo_tien_do_ke_hoach_hop_dong_xay_dung,
            coalesce(`phải_thu_về_cho_vay_ngắn_hạn`, 0) as phai_thu_ve_cho_vay_ngan_han,
            coalesce(`phải_thu_ngắn_hạn_khác`, 0) as phai_thu_ngan_han_khac,
            coalesce(
                `dự_phòng_phải_thu_ngắn_hạn_khó_đòi`, 0
            ) as du_phong_phai_thu_ngan_han_kho_doi,
            coalesce(`tài_sản_thiếu_chờ_xử_lý`, 0) as tai_san_thieu_cho_xu_ly,
            coalesce(`hàng_tồn_kho_ròng`, 0) as hang_ton_kho_rong,
            coalesce(`hàng_tồn_kho`, 0) as hang_ton_kho,
            coalesce(
                `dự_phòng_giảm_giá_hàng_tồn_kho`, 0
            ) as du_phong_giam_gia_hang_ton_kho,
            coalesce(`tài_sản_ngắn_hạn_khác`, 0) as tai_san_ngan_han_khac,
            coalesce(`chi_phí_trả_trước_ngắn_hạn`, 0) as chi_phi_tra_truoc_ngan_han,
            coalesce(
                `thuế_giá_trị_gia_tăng_được_khấu_trừ`, 0
            ) as thue_gia_tri_gia_tang_duoc_khau_tru,
            coalesce(
                `thuế_và_các_khoản_khác_phải_thu_của_nhà_nước`, 0
            ) as thue_va_cac_khoan_khac_phai_thu_cua_nha_nuoc,
            coalesce(
                `giao_dịch_mua_bán_lại_trái_phiếu_chính_phủ`, 0
            ) as giao_dich_mua_ban_lai_trai_phieu_chinh_phu,
            coalesce(`tài_sản_dài_hạn`, 0) as tai_san_dai_han,
            coalesce(`các_khoản_phải_thu_dài_hạn`, 0) as cac_khoan_phai_thu_dai_han,
            coalesce(
                `phải_thu_dài_hạn_của_khách_hàng`, 0
            ) as phai_thu_dai_han_cua_khach_hang,
            coalesce(
                `trả_trước_cho_người_bán_dài_hạn`, 0
            ) as tra_truoc_cho_nguoi_ban_dai_han,
            coalesce(
                `vốn_kinh_doanh_ở_các_đơn_vị_trực_thuộc`, 0
            ) as von_kinh_doanh_o_cac_don_vi_truc_thuoc,
            coalesce(`phải_thu_nội_bộ_dài_hạn`, 0) as phai_thu_noi_bo_dai_han,
            coalesce(`phải_thu_về_cho_vay_dài_hạn`, 0) as phai_thu_ve_cho_vay_dai_han,
            coalesce(`phải_thu_dài_hạn_khác`, 0) as phai_thu_dai_han_khac,
            coalesce(
                `dự_phòng_phải_thu_dài_hạn_khó_đòi`, 0
            ) as du_phong_phai_thu_dai_han_kho_doi,
            coalesce(`tài_sản_cố_định`, 0) as tai_san_co_dinh,
            coalesce(`tài_sản_cố_định_hữu_hình`, 0) as tai_san_co_dinh_huu_hinh,
            coalesce(`nguyên_giá_tscđ_hữu_hình`, 0) as nguyen_gia_tscd_huu_hinh,
            coalesce(
                `khấu_hao_lũy_kế_tscđ_hữu_hình`, 0
            ) as khau_hao_luy_ke_tscd_huu_hinh,
            coalesce(
                `tài_sản_cố_định_thuê_tài_chính`, 0
            ) as tai_san_co_dinh_thue_tai_chinh,
            coalesce(
                `nguyên_giá_tscđ_thuê_tài_chính`, 0
            ) as nguyen_gia_tscd_thue_tai_chinh,
            coalesce(
                `khấu_hao_lũy_kế_tscđ_thuê_tài_chính`, 0
            ) as khau_hao_luy_ke_tscd_thue_tai_chinh,
            coalesce(`tài_sản_cố_định_vô_hình`, 0) as tai_san_co_dinh_vo_hinh,
            coalesce(`nguyên_giá_tscđ_vô_hình`, 0) as nguyen_gia_tscd_vo_hinh,
            coalesce(`khấu_hao_lũy_kế_tscđ_vô_hình`, 0) as khau_hao_luy_ke_tscd_vo_hinh,
            coalesce(`bất_động_sản_đầu_tư`, 0) as bat_dong_san_dau_tu,
            coalesce(`nguyên_giá_bđs_đầu_tư`, 0) as nguyen_gia_bds_dau_tu,
            coalesce(`khấu_hao_lũy_kế_bđs_đầu_tư`, 0) as khau_hao_luy_ke_bds_dau_tu,
            coalesce(`tài_sản_dở_dang_dài_hạn`, 0) as tai_san_do_dang_dai_han,
            coalesce(
                `chi_phí_sản_xuất_kinh_doanh_dở_dang_dài_hạn`, 0
            ) as chi_phi_san_xuat_kinh_doanh_do_dang_dai_han,
            coalesce(
                `chi_phí_xây_dựng_cơ_bản_dở_dang`, 0
            ) as chi_phi_xay_dung_co_ban_do_dang,
            coalesce(`đầu_tư_tài_chính_dài_hạn`, 0) as dau_tu_tai_chinh_dai_han,
            coalesce(`đầu_tư_vào_công_ty_con`, 0) as dau_tu_vao_cong_ty_con,
            coalesce(
                `đầu_tư_vào_công_ty_liên_kết_liên_doanh`, 0
            ) as dau_tu_vao_cong_ty_lien_ket_lien_doanh,
            coalesce(`đầu_tư_dài_hạn_khác`, 0) as dau_tu_dai_han_khac,
            coalesce(
                `dự_phòng_đầu_tư_tài_chính_dài_hạn`, 0
            ) as du_phong_dau_tu_tai_chinh_dai_han,
            coalesce(`tài_sản_dài_hạn_khác`, 0) as tai_san_dai_han_khac,
            coalesce(`chi_phí_trả_trước_dài_hạn`, 0) as chi_phi_tra_truoc_dai_han,
            coalesce(
                `tài_sản_thuế_thu_nhập_hoãn_lại`, 0
            ) as tai_san_thue_thu_nhap_hoan_lai,
            coalesce(
                `thiết_bị_vật_tư_phụ_tùng_thay_thế_dài_hạn`, 0
            ) as thiet_bi_vat_tu_phu_tung_thay_the_dai_han,
            coalesce(`lợi_thế_thương_mại`, 0) as loi_the_thuong_mai,
            coalesce(`tổng_tài_sản`, 0) as tong_tai_san,
            coalesce(`nợ_phải_trả`, 0) as no_phai_tra,
            coalesce(`nợ_ngắn_hạn`, 0) as no_ngan_han,
            coalesce(
                `vay_và_nợ_thuê_tài_chính_ngắn_hạn`, 0
            ) as vay_va_no_thue_tai_chinh_ngan_han,
            coalesce(`phải_trả_người_bán_ngắn_hạn`, 0) as phai_tra_nguoi_ban_ngan_han,
            coalesce(
                `người_mua_trả_tiền_trước_ngắn_hạn`, 0
            ) as nguoi_mua_tra_tien_truoc_ngan_han,
            coalesce(
                `thuế_và_các_khoản_phải_nộp_nhà_nước`, 0
            ) as thue_va_cac_khoan_phai_nop_nha_nuoc,
            coalesce(`phải_trả_người_lao_động`, 0) as phai_tra_nguoi_lao_dong,
            coalesce(`chi_phí_phải_trả_ngắn_hạn`, 0) as chi_phi_phai_tra_ngan_han,
            coalesce(`phải_trả_nội_bộ_ngắn_hạn`, 0) as phai_tra_noi_bo_ngan_han,
            coalesce(
                `phải_trả_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng`, 0
            ) as phai_tra_theo_tien_do_ke_hoach_hop_dong_xay_dung,
            coalesce(
                `doanh_thu_chưa_thực_hiện_ngắn_hạn`, 0
            ) as doanh_thu_chua_thuc_hien_ngan_han,
            coalesce(`phải_trả_khác`, 0) as phai_tra_khac,
            coalesce(
                `dự_phòng_các_khoản_phải_trả_ngắn_hạn`, 0
            ) as du_phong_cac_khoan_phai_tra_ngan_han,
            coalesce(`quỹ_khen_thưởng_phúc_lợi`, 0) as quy_khen_thuong_phuc_loi,
            coalesce(`quỹ_bình_ổn_giá`, 0) as quy_binh_on_gia,
            coalesce(`nợ_dài_hạn`, 0) as no_dai_han,
            coalesce(
                `vay_và_nợ_thuê_tài_chính_dài_hạn`, 0
            ) as vay_va_no_thue_tai_chinh_dai_han,
            coalesce(
                `phải_trả_nhà_cung_cấp_dài_hạn`, 0
            ) as phai_tra_nha_cung_cap_dai_han,
            coalesce(
                `người_mua_trả_tiền_trước_dài_hạn`, 0
            ) as nguoi_mua_tra_tien_truoc_dai_han,
            coalesce(`chi_phí_phải_trả_dài_hạn`, 0) as chi_phi_phai_tra_dai_han,
            coalesce(
                `phải_trả_nội_bộ_về_vốn_kinh_doanh`, 0
            ) as phai_tra_noi_bo_ve_von_kinh_doanh,
            coalesce(`phải_trả_nội_bộ_dài_hạn`, 0) as phai_tra_noi_bo_dai_han,
            coalesce(
                `doanh_thu_chưa_thực_hiện_dài_hạn`, 0
            ) as doanh_thu_chua_thuc_hien_dai_han,
            coalesce(`phải_trả_dài_hạn_khác`, 0) as phai_tra_dai_han_khac,
            coalesce(`trái_phiếu_chuyển_đổi`, 0) as trai_phieu_chuyen_doi,
            coalesce(`cổ_phiếu_ưu_đãi_nợ`, 0) as co_phieu_uu_dai_no,
            coalesce(
                `thuế_thu_nhập_hoãn_lại_phải_trả`, 0
            ) as thue_thu_nhap_hoan_lai_phai_tra,
            coalesce(`dự_phòng_phải_trả_dài_hạn`, 0) as du_phong_phai_tra_dai_han,
            coalesce(
                `quỹ_phát_triển_khoa_học_và_công_nghệ`, 0
            ) as quy_phat_trien_khoa_hoc_va_cong_nghe,
            coalesce(`dự_phòng_trợ_cấp_mất_việc`, 0) as du_phong_tro_cap_mat_viec,
            coalesce(`vốn_chủ_sở_hữu`, 0) as von_chu_so_huu,
            coalesce(`vốn_góp_của_chủ_sở_hữu`, 0) as von_gop_cua_chu_so_huu,
            coalesce(
                `cổ_phiếu_phổ_thông_có_quyền_biểu_quyết`, 0
            ) as co_phieu_pho_thong_co_quyen_bieu_quyet,
            coalesce(`cổ_phiếu_ưu_đãi`, 0) as co_phieu_uu_dai,
            coalesce(`thặng_dư_vốn_cổ_phần`, 0) as thang_du_von_co_phan,
            coalesce(
                `quyền_chọn_chuyển_đổi_trái_phiếu`, 0
            ) as quyen_chon_chuyen_doi_trai_phieu,
            coalesce(`vốn_khác_của_chủ_sở_hữu`, 0) as von_khac_cua_chu_so_huu,
            coalesce(`cổ_phiếu_quỹ`, 0) as co_phieu_quy,
            coalesce(
                `chênh_lệch_đánh_giá_lại_tài_sản`, 0
            ) as chenh_lech_danh_gia_lai_tai_san,
            coalesce(`chênh_lệch_tỷ_giá_hối_đoái`, 0) as chenh_lech_ty_gia_hoi_doai,
            coalesce(`quỹ_đầu_tư_phát_triển`, 0) as quy_dau_tu_phat_trien,
            coalesce(
                `quỹ_hỗ_trợ_sắp_xếp_doanh_nghiệp`, 0
            ) as quy_ho_tro_sap_xep_doanh_nghiep,
            coalesce(
                `quỹ_khác_thuộc_vốn_chủ_sở_hữu`, 0
            ) as quy_khac_thuoc_von_chu_so_huu,
            coalesce(
                `lợi_nhuận_sau_thuế_chưa_phân_phối`, 0
            ) as loi_nhuan_sau_thue_chua_phan_phoi,
            coalesce(
                `lnst_chưa_phân_phối_lũy_kế_đến_cuối_kỳ_trước`, 0
            ) as lnst_chua_phan_phoi_luy_ke_den_cuoi_ky_truoc,
            coalesce(`lnst_chưa_phân_phối_kỳ_này`, 0) as lnst_chua_phan_phoi_ky_nay,
            coalesce(
                `lợi_ích_cổ_đông_không_kiểm_soát`, 0
            ) as loi_ich_co_dong_khong_kiem_soat,
            coalesce(`nguồn_kinh_phí_và_quỹ_khác`, 0) as nguon_kinh_phi_va_quy_khac,
            coalesce(`tổng_nguồn_vốn`, 0) as tong_nguon_von,
            coalesce(`tiền_mặt_vàng_bạc_đá_quý`, 0) as tien_mat_vang_bac_da_quy,
            coalesce(
                `tiền_gửi_tại_ngân_hàng_nhà_nước`, 0
            ) as tien_gui_tai_ngan_hang_nha_nuoc,
            coalesce(
                `tiền_vàng_gửi_tại_các_tổ_chức_tín_dụng_khác_và_cho_vay_các_tổ_chức_tín_dụng_khác`,
                0
            )
            as tien_vang_gui_tai_cac_to_chuc_tin_dung_khac_va_cho_vay_cac_to_chuc_tin_dung_khac,
            coalesce(
                `các_công_cụ_tài_chính_phái_sinh_và_các_tài_sản_tài_chính_khác`, 0
            ) as cac_cong_cu_tai_chinh_phai_sinh_va_cac_tai_san_tai_chinh_khac,
            coalesce(`cho_vay_khách_hàng`, 0) as cho_vay_khach_hang,
            coalesce(
                `cho_vay_và_cho_thuê_tài_chính_khách_hàng`, 0
            ) as cho_vay_va_cho_thue_tai_chinh_khach_hang,
            coalesce(
                `dự_phòng_rủi_ro_cho_vay_và_cho_thuê_tài_chính_khách_hàng`, 0
            ) as du_phong_rui_ro_cho_vay_va_cho_thue_tai_chinh_khach_hang,
            coalesce(`hoạt_động_mua_nợ`, 0) as hoat_dong_mua_no,
            coalesce(`mua_nợ`, 0) as mua_no,
            coalesce(
                `dự_phòng_rủi_ro_hoạt_động_mua_nợ`, 0
            ) as du_phong_rui_ro_hoat_dong_mua_no,
            coalesce(`chứng_khoán_đầu_tư`, 0) as chung_khoan_dau_tu,
            coalesce(
                `chứng_khoán_đầu_tư_sẵn_sàng_để_bán`, 0
            ) as chung_khoan_dau_tu_san_sang_de_ban,
            coalesce(
                `chứng_khoán_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn`, 0
            ) as chung_khoan_dau_tu_nam_giu_den_ngay_dao_han,
            coalesce(
                `dự_phòng_giảm_giá_chứng_khoán_đầu_tư`, 0
            ) as du_phong_giam_gia_chung_khoan_dau_tu,
            coalesce(`góp_vốn_đầu_tư_dài_hạn`, 0) as gop_von_dau_tu_dai_han,
            coalesce(
                `dự_phòng_giảm_giá_đầu_tư_dài_hạn`, 0
            ) as du_phong_giam_gia_dau_tu_dai_han,
            coalesce(
                `đầu_tư_vào_công_ty_liên_doanh_liên_kết`, 0
            ) as dau_tu_vao_cong_ty_lien_doanh_lien_ket,
            coalesce(`tài_sản_“có”_khác`, 0) as tai_san_co_khac,
            coalesce(`tổng_nợ_phải_trả`, 0) as tong_no_phai_tra,
            coalesce(
                `các_khoản_nợ_chính_phủ_và_ngân_hàng_nhà_nước`, 0
            ) as cac_khoan_no_chinh_phu_va_ngan_hang_nha_nuoc,
            coalesce(
                `tiền_gửi_và_vay_các_tổ_chức_tín_dụng_khác`, 0
            ) as tien_gui_va_vay_cac_to_chuc_tin_dung_khac,
            coalesce(`tiền_gửi_của_khách_hàng`, 0) as tien_gui_cua_khach_hang,
            coalesce(
                `các_công_cụ_tài_chính_phái_sinh_và_các_khoản_nợ_tài_chính_khác`, 0
            ) as cac_cong_cu_tai_chinh_phai_sinh_va_cac_khoan_no_tai_chinh_khac,
            coalesce(
                `vốn_tài_trợ_ủy_thác_đầu_tư_cho_vay_mà_tổ_chức_tín_dụng_chịu_rủi_ro`, 0
            ) as von_tai_tro_uy_thac_dau_tu_cho_vay_ma_to_chuc_tin_dung_chiu_rui_ro,
            coalesce(`phát_hành_giấy_tờ_có_giá`, 0) as phat_hanh_giay_to_co_gia,
            coalesce(`các_khoản_nợ_khác`, 0) as cac_khoan_no_khac,
            coalesce(`vốn_của_tổ_chức_tín_dụng`, 0) as von_cua_to_chuc_tin_dung,
            coalesce(`vốn_đầu_tư_xây_dựng_cơ_bản`, 0) as von_dau_tu_xay_dung_co_ban,
            coalesce(`vốn_khác`, 0) as von_khac,
            coalesce(`quỹ_của_tổ_chức_tín_dụng`, 0) as quy_cua_to_chuc_tin_dung,
            coalesce(
                `giá_trị_thuần_đầu_tư_tài_sản_tài_chính_ngắn_hạn`, 0
            ) as gia_tri_thuan_dau_tu_tai_san_tai_chinh_ngan_han,
            coalesce(
                `các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`, 0
            ) as cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            coalesce(
                `các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`, 0
            ) as cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            coalesce(`các_khoản_cho_vay`, 0) as cac_khoan_cho_vay,
            coalesce(
                `các_khoản_tài_chính_sẵn_sàng_để_bán_afs`, 0
            ) as cac_khoan_tai_chinh_san_sang_de_ban_afs,
            coalesce(`tổng_các_khoản_phải_thu`, 0) as tong_cac_khoan_phai_thu,
            coalesce(`các_khoản_phải_thu`, 0) as cac_khoan_phai_thu,
            coalesce(
                `phải_thu_các_dịch_vụ_công_ty_chứng_khoán_cung_cấp`, 0
            ) as phai_thu_cac_dich_vu_cong_ty_chung_khoan_cung_cap,
            coalesce(
                `phải_thu_về_lỗi_giao_dịch_chứng_khoán`, 0
            ) as phai_thu_ve_loi_giao_dich_chung_khoan,
            coalesce(
                `phải_thu_về_hoạt_động_giao_dịch_chứng_khoán`, 0
            ) as phai_thu_ve_hoat_dong_giao_dich_chung_khoan,
            coalesce(`phải_thu_về_xây_dựng_cơ_bản`, 0) as phai_thu_ve_xay_dung_co_ban,
            coalesce(
                `dự_phòng_suy_giảm_giá_trị_các_khoản_phải_thu`, 0
            ) as du_phong_suy_giam_gia_tri_cac_khoan_phai_thu,
            coalesce(`dự_phòng_nợ_khó_đòi`, 0) as du_phong_no_kho_doi,
            coalesce(
                `vật_tư_văn_phòng_công_cụ_dụng_cụ`, 0
            ) as vat_tu_van_phong_cong_cu_dung_cu,
            coalesce(
                `các_khoản_cầm_cố_ký_cược_ký_quỹ`, 0
            ) as cac_khoan_cam_co_ky_cuoc_ky_quy,
            coalesce(`phải_thu_thuế_khác`, 0) as phai_thu_thue_khac,
            coalesce(
                `dự_phòng_suy_giảm_giá_trị_tài_sản_ngắn_hạn_khác`, 0
            ) as du_phong_suy_giam_gia_tri_tai_san_ngan_han_khac,
            coalesce(
                `dự_phòng_giảm_giá_tài_sản_tài_chính_dài_hạn`, 0
            ) as du_phong_giam_gia_tai_san_tai_chinh_dai_han,
            coalesce(`đầu_tư_dài_hạn`, 0) as dau_tu_dai_han,
            coalesce(`đầu_tư_chứng_khoán_dài_hạn`, 0) as dau_tu_chung_khoan_dai_han,
            coalesce(
                `cầm_cố_thế_chấp_ký_quỹ_và_ký_cược_dài_hạn`, 0
            ) as cam_co_the_chap_ky_quy_va_ky_cuoc_dai_han,
            coalesce(
                `tiền_chi_nộp_quỹ_hỗ_trợ_thanh_toán`, 0
            ) as tien_chi_nop_quy_ho_tro_thanh_toan,
            coalesce(
                `dự_phòng_suy_giảm_giá_trị_tài_sản_dài_hạn`, 0
            ) as du_phong_suy_giam_gia_tri_tai_san_dai_han,
            coalesce(
                `vay_tài_sản_tài_chính_ngắn_hạn`, 0
            ) as vay_tai_san_tai_chinh_ngan_han,
            coalesce(
                `trái_phiếu_chuyển_đổi_ngắn_hạn`, 0
            ) as trai_phieu_chuyen_doi_ngan_han,
            coalesce(
                `trái_phiếu_phát_hành_ngắn_hạn`, 0
            ) as trai_phieu_phat_hanh_ngan_han,
            coalesce(`vay_quỹ_hỗ_trợ_thanh_toán`, 0) as vay_quy_ho_tro_thanh_toan,
            coalesce(
                `phải_trả_hoạt_động_giao_dịch_chứng_khoán`, 0
            ) as phai_tra_hoat_dong_giao_dich_chung_khoan,
            coalesce(
                `phải_trả_về_lỗi_giao_dịch_các_tài_sản_tài_chính`, 0
            ) as phai_tra_ve_loi_giao_dich_cac_tai_san_tai_chinh,
            coalesce(`các_khoản_phải_trả_về_thuế`, 0) as cac_khoan_phai_tra_ve_thue,
            coalesce(
                `các_khoản_trích_nộp_phúc_lợi_nhân_viên`, 0
            ) as cac_khoan_trich_nop_phuc_loi_nhan_vien,
            coalesce(`nhận_ký_quỹ_ký_cược_ngắn_hạn`, 0) as nhan_ky_quy_ky_cuoc_ngan_han,
            coalesce(`phải_trả_về_xây_dựng_cơ_bản`, 0) as phai_tra_ve_xay_dung_co_ban,
            coalesce(
                `phải_trả_cổ_tức_gốc_và_lãi_trái_phiếu`, 0
            ) as phai_tra_co_tuc_goc_va_lai_trai_phieu,
            coalesce(
                `phải_trả_tổ_chức_phát_hành_chứng_khoán`, 0
            ) as phai_tra_to_chuc_phat_hanh_chung_khoan,
            coalesce(
                `vay_tài_sản_tài_chính_dài_hạn`, 0
            ) as vay_tai_san_tai_chinh_dai_han,
            coalesce(
                `trái_phiếu_chuyển_đổi_dài_hạn`, 0
            ) as trai_phieu_chuyen_doi_dai_han,
            coalesce(`trái_phiếu_phát_hành_dài_hạn`, 0) as trai_phieu_phat_hanh_dai_han,
            coalesce(`ký_quỹ_ký_cược_dài_hạn`, 0) as ky_quy_ky_cuoc_dai_han,
            coalesce(`quỹ_bảo_vệ_nhà_đầu_tư`, 0) as quy_bao_ve_nha_dau_tu,
            coalesce(`quỹ_dự_trữ_điều_lệ`, 0) as quy_du_tru_dieu_le,
            coalesce(`quỹ_dự_phòng_tài_chính`, 0) as quy_du_phong_tai_chinh,
            coalesce(
                `phải_thu_về_hợp_đồng_bảo_hiểm`, 0
            ) as phai_thu_ve_hop_dong_bao_hiem,
            coalesce(`phải_thu_khác_của_khách_hàng`, 0) as phai_thu_khac_cua_khach_hang,
            coalesce(`tạm_ứng`, 0) as tam_ung,
            coalesce(
                `phải_thu_từ_hoạt_động_đầu_tư_tài_chính`, 0
            ) as phai_thu_tu_hoat_dong_dau_tu_tai_chinh,
            coalesce(
                `chi_phí_hoa_hồng_chưa_phân_bổ`, 0
            ) as chi_phi_hoa_hong_chua_phan_bo,
            coalesce(
                `chi_phí_trả_trước_ngắn_hạn_khác`, 0
            ) as chi_phi_tra_truoc_ngan_han_khac,
            coalesce(`tài_sản_tái_bảo_hiểm`, 0) as tai_san_tai_bao_hiem,
            coalesce(
                `dự_phòng_phí_nhượng_tái_bảo_hiểm`, 0
            ) as du_phong_phi_nhuong_tai_bao_hiem,
            coalesce(
                `dự_phòng_bồi_thường_nhượng_tái_bảo_hiểm`, 0
            ) as du_phong_boi_thuong_nhuong_tai_bao_hiem,
            coalesce(`ký_quỹ_bảo_hiểm`, 0) as ky_quy_bao_hiem,
            coalesce(`tài_sản_ký_quỹ_dài_hạn`, 0) as tai_san_ky_quy_dai_han,
            coalesce(`vay_và_nợ_ngắn_hạn`, 0) as vay_va_no_ngan_han,
            coalesce(`phải_trả_thương_mại`, 0) as phai_tra_thuong_mai,
            coalesce(
                `phải_trả_về_hợp_đồng_bảo_hiểm`, 0
            ) as phai_tra_ve_hop_dong_bao_hiem,
            coalesce(`phải_trả_khác_cho_người_bán`, 0) as phai_tra_khac_cho_nguoi_ban,
            coalesce(
                `doanh_thu_hoa_hồng_chưa_được_hưởng`, 0
            ) as doanh_thu_hoa_hong_chua_duoc_huong,
            coalesce(`dự_phòng_nghiệp_vụ_bảo_hiểm`, 0) as du_phong_nghiep_vu_bao_hiem,
            coalesce(
                `dự_phòng_phí_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`, 0
            ) as du_phong_phi_bao_hiem_goc_va_nhan_tai_bao_hiem,
            coalesce(`dự_phòng_toán_học`, 0) as du_phong_toan_hoc,
            coalesce(
                `dự_phòng_bồi_thường_bảo_hiểm_gốc_nhận_tái_bảo_hiểm`, 0
            ) as du_phong_boi_thuong_bao_hiem_goc_nhan_tai_bao_hiem,
            coalesce(`dự_phòng_dao_động_lớn`, 0) as du_phong_dao_dong_lon,
            coalesce(`dự_phòng_chia_lãi`, 0) as du_phong_chia_lai,
            coalesce(`dự_phòng_đảm_bảo_cân_đối`, 0) as du_phong_dam_bao_can_doi,
            coalesce(`quỹ_dự_trữ_bắt_buộc`, 0) as quy_du_tru_bat_buoc,
            coalesce(`vốn_điều_lệ`, 0) as von_dieu_le,
            --
            tien_va_cac_khoan_tuong_duong_tien  -- Non-Financial, Bao Hiem, Chung Khoan
            + tien_mat_vang_bac_da_quy  -- Ngan Hang
            + tien_gui_tai_ngan_hang_nha_nuoc  -- Ngan Hang
            + tien_vang_gui_tai_cac_to_chuc_tin_dung_khac_va_cho_vay_cac_to_chuc_tin_dung_khac  -- Ngan Hang
            + dau_tu_nam_giu_den_ngay_dao_han  -- Non-Financial, Bao Hiem
            + cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm  -- Chung Khoan
            + chung_khoan_dau_tu_nam_giu_den_ngay_dao_han  -- Ngan Hang
            as cash_and_marketable_securities,

            tien_va_cac_khoan_tuong_duong_tien  -- Non-Financial, Bao Hiem, Chung Khoan
            + tien_mat_vang_bac_da_quy  -- Ngan Hang
            + tien_gui_tai_ngan_hang_nha_nuoc  -- Ngan Hang
            + tien_vang_gui_tai_cac_to_chuc_tin_dung_khac_va_cho_vay_cac_to_chuc_tin_dung_khac  -- Ngan Hang
            + dau_tu_tai_chinh_ngan_han  -- Non-Financial, Bao Hiem
            + cac_khoan_phai_thu_ngan_han  -- Non-Financial, Bao Hiem
            + gia_tri_thuan_dau_tu_tai_san_tai_chinh_ngan_han  -- Chung Khoan
            + tong_cac_khoan_phai_thu  -- Chung Khoan
            as quick_assets,

            vay_va_no_thue_tai_chinh_dai_han  -- Non-Financial, Chung Khoan, Bao Hiem
            + trai_phieu_chuyen_doi  -- Non-Financial, Bao Hiem
            + co_phieu_uu_dai_no  -- Non-Financial, Chung Khoan, Bao Hiem
            + co_phieu_uu_dai  -- Non-Financial, Chung Khoan, Bao Hiem
            + vay_tai_san_tai_chinh_dai_han  -- Chung Khoan
            + trai_phieu_chuyen_doi_dai_han  -- Chung Khoan
            + trai_phieu_phat_hanh_dai_han  -- Chung Khoan
            as long_term_debt,

            vay_va_no_thue_tai_chinh_ngan_han  -- Non-Financial, Chung Khoan
            + vay_va_no_ngan_han  -- Bao Hiem
            + vay_tai_san_tai_chinh_ngan_han  -- Chung Khoan
            + trai_phieu_chuyen_doi_ngan_han  -- Chung Khoan
            + trai_phieu_phat_hanh_ngan_han  -- Chung Khoan
            + vay_quy_ho_tro_thanh_toan  -- Chung Khoan
            as short_term_debt,

            long_term_debt
            + short_term_debt
            + cac_khoan_no_chinh_phu_va_ngan_hang_nha_nuoc  -- Ngan Hang
            + tien_gui_va_vay_cac_to_chuc_tin_dung_khac  -- Ngan Hang
            + cac_cong_cu_tai_chinh_phai_sinh_va_cac_khoan_no_tai_chinh_khac  -- Ngan Hang
            + phat_hanh_giay_to_co_gia  -- Ngan Hang
            as total_debt,

            cash_and_marketable_securities - total_debt as net_cash,

            tong_no_phai_tra + no_phai_tra as total_liabilities,
            tong_tai_san as total_assets,
            von_chu_so_huu as total_equity,
            total_equity + total_debt as total_invested_capital,
            case
                when tai_san_dai_han > 0
                then tai_san_dai_han
                else gop_von_dau_tu_dai_han + tai_san_co_dinh + bat_dong_san_dau_tu
            end as total_fixed_assets
        from source
    )

select *
from renamed
