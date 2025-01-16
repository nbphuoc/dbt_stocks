with
    source as (select * from {{ source("simplize", "fct_bs") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            quarter as quarter,
            date_trunc(
                'QUARTER',
                make_date(
                    cast(substring(quarter, 1, 4) as int),
                    (cast(substring(quarter, 6, 1) as int) - 1) * 3 + 1,
                    1
                )
            )::date as quarter_start_date,
            `tài_sản_ngắn_hạn` as tai_san_ngan_han,
            `tiền_và_các_khoản_tương_đương_tiền` as tien_va_cac_khoan_tuong_duong_tien,
            `tiền` as tien,
            `các_khoản_tương_đương_tiền` as cac_khoan_tuong_duong_tien,
            `đầu_tư_tài_chính_ngắn_hạn` as dau_tu_tai_chinh_ngan_han,
            `chứng_khoán_kinh_doanh` as chung_khoan_kinh_doanh,
            `dự_phòng_giảm_giá_chứng_khoán_kinh_doanh`
            as du_phong_giam_gia_chung_khoan_kinh_doanh,
            `đầu_tư_nắm_giữ_đến_ngày_đáo_hạn` as dau_tu_nam_giu_den_ngay_dao_han,
            `các_khoản_phải_thu_ngắn_hạn` as cac_khoan_phai_thu_ngan_han,
            `phải_thu_ngắn_hạn_của_khách_hàng` as phai_thu_ngan_han_cua_khach_hang,
            `trả_trước_cho_người_bán_ngắn_hạn` as tra_truoc_cho_nguoi_ban_ngan_han,
            `phải_thu_nội_bộ_ngắn_hạn` as phai_thu_noi_bo_ngan_han,
            `phải_thu_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng`
            as phai_thu_theo_tien_do_ke_hoach_hop_dong_xay_dung,
            `phải_thu_về_cho_vay_ngắn_hạn` as phai_thu_ve_cho_vay_ngan_han,
            `phải_thu_ngắn_hạn_khác` as phai_thu_ngan_han_khac,
            `dự_phòng_phải_thu_ngắn_hạn_khó_đòi` as du_phong_phai_thu_ngan_han_kho_doi,
            `tài_sản_thiếu_chờ_xử_lý` as tai_san_thieu_cho_xu_ly,
            `hàng_tồn_kho_ròng` as hang_ton_kho_rong,
            `hàng_tồn_kho` as hang_ton_kho,
            `dự_phòng_giảm_giá_hàng_tồn_kho` as du_phong_giam_gia_hang_ton_kho,
            `tài_sản_ngắn_hạn_khác` as tai_san_ngan_han_khac,
            `chi_phí_trả_trước_ngắn_hạn` as chi_phi_tra_truoc_ngan_han,
            `thuế_giá_trị_gia_tăng_được_khấu_trừ`
            as thue_gia_tri_gia_tang_duoc_khau_tru,
            `thuế_và_các_khoản_khác_phải_thu_của_nhà_nước`
            as thue_va_cac_khoan_khac_phai_thu_cua_nha_nuoc,
            `giao_dịch_mua_bán_lại_trái_phiếu_chính_phủ`
            as giao_dich_mua_ban_lai_trai_phieu_chinh_phu,
            `tài_sản_dài_hạn` as tai_san_dai_han,
            `các_khoản_phải_thu_dài_hạn` as cac_khoan_phai_thu_dai_han,
            `phải_thu_dài_hạn_của_khách_hàng` as phai_thu_dai_han_cua_khach_hang,
            `trả_trước_cho_người_bán_dài_hạn` as tra_truoc_cho_nguoi_ban_dai_han,
            `vốn_kinh_doanh_ở_các_đơn_vị_trực_thuộc`
            as von_kinh_doanh_o_cac_don_vi_truc_thuoc,
            `phải_thu_nội_bộ_dài_hạn` as phai_thu_noi_bo_dai_han,
            `phải_thu_về_cho_vay_dài_hạn` as phai_thu_ve_cho_vay_dai_han,
            `phải_thu_dài_hạn_khác` as phai_thu_dai_han_khac,
            `dự_phòng_phải_thu_dài_hạn_khó_đòi` as du_phong_phai_thu_dai_han_kho_doi,
            `tài_sản_cố_định` as tai_san_co_dinh,
            `tài_sản_cố_định_hữu_hình` as tai_san_co_dinh_huu_hinh,
            `nguyên_giá_tscđ_hữu_hình` as nguyen_gia_tscd_huu_hinh,
            `khấu_hao_lũy_kế_tscđ_hữu_hình` as khau_hao_luy_ke_tscd_huu_hinh,
            `tài_sản_cố_định_thuê_tài_chính` as tai_san_co_dinh_thue_tai_chinh,
            `nguyên_giá_tscđ_thuê_tài_chính` as nguyen_gia_tscd_thue_tai_chinh,
            `khấu_hao_lũy_kế_tscđ_thuê_tài_chính`
            as khau_hao_luy_ke_tscd_thue_tai_chinh,
            `tài_sản_cố_định_vô_hình` as tai_san_co_dinh_vo_hinh,
            `nguyên_giá_tscđ_vô_hình` as nguyen_gia_tscd_vo_hinh,
            `khấu_hao_lũy_kế_tscđ_vô_hình` as khau_hao_luy_ke_tscd_vo_hinh,
            `bất_động_sản_đầu_tư` as bat_dong_san_dau_tu,
            `nguyên_giá_bđs_đầu_tư` as nguyen_gia_bds_dau_tu,
            `khấu_hao_lũy_kế_bđs_đầu_tư` as khau_hao_luy_ke_bds_dau_tu,
            `tài_sản_dở_dang_dài_hạn` as tai_san_do_dang_dai_han,
            `chi_phí_sản_xuất_kinh_doanh_dở_dang_dài_hạn`
            as chi_phi_san_xuat_kinh_doanh_do_dang_dai_han,
            `chi_phí_xây_dựng_cơ_bản_dở_dang` as chi_phi_xay_dung_co_ban_do_dang,
            `đầu_tư_tài_chính_dài_hạn` as dau_tu_tai_chinh_dai_han,
            `đầu_tư_vào_công_ty_con` as dau_tu_vao_cong_ty_con,
            `đầu_tư_vào_công_ty_liên_kết_liên_doanh`
            as dau_tu_vao_cong_ty_lien_ket_lien_doanh,
            `đầu_tư_dài_hạn_khác` as dau_tu_dai_han_khac,
            `dự_phòng_đầu_tư_tài_chính_dài_hạn` as du_phong_dau_tu_tai_chinh_dai_han,
            `tài_sản_dài_hạn_khác` as tai_san_dai_han_khac,
            `chi_phí_trả_trước_dài_hạn` as chi_phi_tra_truoc_dai_han,
            `tài_sản_thuế_thu_nhập_hoãn_lại` as tai_san_thue_thu_nhap_hoan_lai,
            `thiết_bị_vật_tư_phụ_tùng_thay_thế_dài_hạn`
            as thiet_bi_vat_tu_phu_tung_thay_the_dai_han,
            `lợi_thế_thương_mại` as loi_the_thuong_mai,
            `tổng_tài_sản` as tong_tai_san,
            `nợ_phải_trả` as no_phai_tra,
            `nợ_ngắn_hạn` as no_ngan_han,
            `vay_và_nợ_thuê_tài_chính_ngắn_hạn` as vay_va_no_thue_tai_chinh_ngan_han,
            `phải_trả_người_bán_ngắn_hạn` as phai_tra_nguoi_ban_ngan_han,
            `người_mua_trả_tiền_trước_ngắn_hạn` as nguoi_mua_tra_tien_truoc_ngan_han,
            `thuế_và_các_khoản_phải_nộp_nhà_nước`
            as thue_va_cac_khoan_phai_nop_nha_nuoc,
            `phải_trả_người_lao_động` as phai_tra_nguoi_lao_dong,
            `chi_phí_phải_trả_ngắn_hạn` as chi_phi_phai_tra_ngan_han,
            `phải_trả_nội_bộ_ngắn_hạn` as phai_tra_noi_bo_ngan_han,
            `phải_trả_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng`
            as phai_tra_theo_tien_do_ke_hoach_hop_dong_xay_dung,
            `doanh_thu_chưa_thực_hiện_ngắn_hạn` as doanh_thu_chua_thuc_hien_ngan_han,
            `phải_trả_khác` as phai_tra_khac,
            `dự_phòng_các_khoản_phải_trả_ngắn_hạn`
            as du_phong_cac_khoan_phai_tra_ngan_han,
            `quỹ_khen_thưởng_phúc_lợi` as quy_khen_thuong_phuc_loi,
            `quỹ_bình_ổn_giá` as quy_binh_on_gia,
            `nợ_dài_hạn` as no_dai_han,
            `vay_và_nợ_thuê_tài_chính_dài_hạn` as vay_va_no_thue_tai_chinh_dai_han,
            `phải_trả_nhà_cung_cấp_dài_hạn` as phai_tra_nha_cung_cap_dai_han,
            `người_mua_trả_tiền_trước_dài_hạn` as nguoi_mua_tra_tien_truoc_dai_han,
            `chi_phí_phải_trả_dài_hạn` as chi_phi_phai_tra_dai_han,
            `phải_trả_nội_bộ_về_vốn_kinh_doanh` as phai_tra_noi_bo_ve_von_kinh_doanh,
            `phải_trả_nội_bộ_dài_hạn` as phai_tra_noi_bo_dai_han,
            `doanh_thu_chưa_thực_hiện_dài_hạn` as doanh_thu_chua_thuc_hien_dai_han,
            `phải_trả_dài_hạn_khác` as phai_tra_dai_han_khac,
            `trái_phiếu_chuyển_đổi` as trai_phieu_chuyen_doi,
            `cổ_phiếu_ưu_đãi_nợ` as co_phieu_uu_dai_no,
            `thuế_thu_nhập_hoãn_lại_phải_trả` as thue_thu_nhap_hoan_lai_phai_tra,
            `dự_phòng_phải_trả_dài_hạn` as du_phong_phai_tra_dai_han,
            `quỹ_phát_triển_khoa_học_và_công_nghệ`
            as quy_phat_trien_khoa_hoc_va_cong_nghe,
            `dự_phòng_trợ_cấp_mất_việc` as du_phong_tro_cap_mat_viec,
            `vốn_chủ_sở_hữu` as von_chu_so_huu,
            `vốn_góp_của_chủ_sở_hữu` as von_gop_cua_chu_so_huu,
            `cổ_phiếu_phổ_thông_có_quyền_biểu_quyết`
            as co_phieu_pho_thong_co_quyen_bieu_quyet,
            `cổ_phiếu_ưu_đãi` as co_phieu_uu_dai,
            `thặng_dư_vốn_cổ_phần` as thang_du_von_co_phan,
            `quyền_chọn_chuyển_đổi_trái_phiếu` as quyen_chon_chuyen_doi_trai_phieu,
            `vốn_khác_của_chủ_sở_hữu` as von_khac_cua_chu_so_huu,
            `cổ_phiếu_quỹ` as co_phieu_quy,
            `chênh_lệch_đánh_giá_lại_tài_sản` as chenh_lech_danh_gia_lai_tai_san,
            `chênh_lệch_tỷ_giá_hối_đoái` as chenh_lech_ty_gia_hoi_doai,
            `quỹ_đầu_tư_phát_triển` as quy_dau_tu_phat_trien,
            `quỹ_hỗ_trợ_sắp_xếp_doanh_nghiệp` as quy_ho_tro_sap_xep_doanh_nghiep,
            `quỹ_khác_thuộc_vốn_chủ_sở_hữu` as quy_khac_thuoc_von_chu_so_huu,
            `lợi_nhuận_sau_thuế_chưa_phân_phối` as loi_nhuan_sau_thue_chua_phan_phoi,
            `lnst_chưa_phân_phối_lũy_kế_đến_cuối_kỳ_trước`
            as lnst_chua_phan_phoi_luy_ke_den_cuoi_ky_truoc,
            `lnst_chưa_phân_phối_kỳ_này` as lnst_chua_phan_phoi_ky_nay,
            `lợi_ích_cổ_đông_không_kiểm_soát` as loi_ich_co_dong_khong_kiem_soat,
            `nguồn_kinh_phí_và_quỹ_khác` as nguon_kinh_phi_va_quy_khac,
            `tổng_nguồn_vốn` as tong_nguon_von,
            `tiền_mặt_vàng_bạc_đá_quý` as tien_mat_vang_bac_da_quy,
            `tiền_gửi_tại_ngân_hàng_nhà_nước` as tien_gui_tai_ngan_hang_nha_nuoc,
            `tiền_vàng_gửi_tại_các_tổ_chức_tín_dụng_khác_và_cho_vay_các_tổ_chức_tín_dụng_khác`
            as tien_vang_gui_tai_cac_to_chuc_tin_dung_khac_va_cho_vay_cac_to_chuc_tin_dung_khac,
            `các_công_cụ_tài_chính_phái_sinh_và_các_tài_sản_tài_chính_khác`
            as cac_cong_cu_tai_chinh_phai_sinh_va_cac_tai_san_tai_chinh_khac,
            `cho_vay_khách_hàng` as cho_vay_khach_hang,
            `cho_vay_và_cho_thuê_tài_chính_khách_hàng`
            as cho_vay_va_cho_thue_tai_chinh_khach_hang,
            `dự_phòng_rủi_ro_cho_vay_và_cho_thuê_tài_chính_khách_hàng`
            as du_phong_rui_ro_cho_vay_va_cho_thue_tai_chinh_khach_hang,
            `hoạt_động_mua_nợ` as hoat_dong_mua_no,
            `mua_nợ` as mua_no,
            `dự_phòng_rủi_ro_hoạt_động_mua_nợ` as du_phong_rui_ro_hoat_dong_mua_no,
            `chứng_khoán_đầu_tư` as chung_khoan_dau_tu,
            `chứng_khoán_đầu_tư_sẵn_sàng_để_bán` as chung_khoan_dau_tu_san_sang_de_ban,
            `chứng_khoán_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn`
            as chung_khoan_dau_tu_nam_giu_den_ngay_dao_han,
            `dự_phòng_giảm_giá_chứng_khoán_đầu_tư`
            as du_phong_giam_gia_chung_khoan_dau_tu,
            `góp_vốn_đầu_tư_dài_hạn` as gop_von_dau_tu_dai_han,
            `dự_phòng_giảm_giá_đầu_tư_dài_hạn` as du_phong_giam_gia_dau_tu_dai_han,
            `đầu_tư_vào_công_ty_liên_doanh_liên_kết`
            as dau_tu_vao_cong_ty_lien_doanh_lien_ket,
            `tài_sản_“có”_khác` as tai_san_co_khac,
            `tổng_nợ_phải_trả` as tong_no_phai_tra,
            `các_khoản_nợ_chính_phủ_và_ngân_hàng_nhà_nước`
            as cac_khoan_no_chinh_phu_va_ngan_hang_nha_nuoc,
            `tiền_gửi_và_vay_các_tổ_chức_tín_dụng_khác`
            as tien_gui_va_vay_cac_to_chuc_tin_dung_khac,
            `tiền_gửi_của_khách_hàng` as tien_gui_cua_khach_hang,
            `các_công_cụ_tài_chính_phái_sinh_và_các_khoản_nợ_tài_chính_khác`
            as cac_cong_cu_tai_chinh_phai_sinh_va_cac_khoan_no_tai_chinh_khac,
            `vốn_tài_trợ_ủy_thác_đầu_tư_cho_vay_mà_tổ_chức_tín_dụng_chịu_rủi_ro`
            as von_tai_tro_uy_thac_dau_tu_cho_vay_ma_to_chuc_tin_dung_chiu_rui_ro,
            `phát_hành_giấy_tờ_có_giá` as phat_hanh_giay_to_co_gia,
            `các_khoản_nợ_khác` as cac_khoan_no_khac,
            `vốn_của_tổ_chức_tín_dụng` as von_cua_to_chuc_tin_dung,
            `vốn_điều_lệ` as von_dieu_le,
            `vốn_đầu_tư_xây_dựng_cơ_bản` as von_dau_tu_xay_dung_co_ban,
            `vốn_khác` as von_khac,
            `quỹ_của_tổ_chức_tín_dụng` as quy_cua_to_chuc_tin_dung,
            `giá_trị_thuần_đầu_tư_tài_sản_tài_chính_ngắn_hạn`
            as gia_tri_thuan_dau_tu_tai_san_tai_chinh_ngan_han,
            `các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`
            as cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            `các_khoản_cho_vay` as cac_khoan_cho_vay,
            `các_khoản_tài_chính_sẵn_sàng_để_bán_afs`
            as cac_khoan_tai_chinh_san_sang_de_ban_afs,
            `tổng_các_khoản_phải_thu` as tong_cac_khoan_phai_thu,
            `các_khoản_phải_thu` as cac_khoan_phai_thu,
            `phải_thu_các_dịch_vụ_công_ty_chứng_khoán_cung_cấp`
            as phai_thu_cac_dich_vu_cong_ty_chung_khoan_cung_cap,
            `phải_thu_về_lỗi_giao_dịch_chứng_khoán`
            as phai_thu_ve_loi_giao_dich_chung_khoan,
            `phải_thu_về_hoạt_động_giao_dịch_chứng_khoán`
            as phai_thu_ve_hoat_dong_giao_dich_chung_khoan,
            `phải_thu_về_xây_dựng_cơ_bản` as phai_thu_ve_xay_dung_co_ban,
            `dự_phòng_suy_giảm_giá_trị_các_khoản_phải_thu`
            as du_phong_suy_giam_gia_tri_cac_khoan_phai_thu,
            `dự_phòng_nợ_khó_đòi` as du_phong_no_kho_doi,
            `vật_tư_văn_phòng_công_cụ_dụng_cụ` as vat_tu_van_phong_cong_cu_dung_cu,
            `các_khoản_cầm_cố_ký_cược_ký_quỹ` as cac_khoan_cam_co_ky_cuoc_ky_quy,
            `phải_thu_thuế_khác` as phai_thu_thue_khac,
            `dự_phòng_suy_giảm_giá_trị_tài_sản_ngắn_hạn_khác`
            as du_phong_suy_giam_gia_tri_tai_san_ngan_han_khac,
            `dự_phòng_giảm_giá_tài_sản_tài_chính_dài_hạn`
            as du_phong_giam_gia_tai_san_tai_chinh_dai_han,
            `đầu_tư_dài_hạn` as dau_tu_dai_han,
            `đầu_tư_chứng_khoán_dài_hạn` as dau_tu_chung_khoan_dai_han,
            `cầm_cố_thế_chấp_ký_quỹ_và_ký_cược_dài_hạn`
            as cam_co_the_chap_ky_quy_va_ky_cuoc_dai_han,
            `tiền_chi_nộp_quỹ_hỗ_trợ_thanh_toán` as tien_chi_nop_quy_ho_tro_thanh_toan,
            `dự_phòng_suy_giảm_giá_trị_tài_sản_dài_hạn`
            as du_phong_suy_giam_gia_tri_tai_san_dai_han,
            `vay_tài_sản_tài_chính_ngắn_hạn` as vay_tai_san_tai_chinh_ngan_han,
            `trái_phiếu_chuyển_đổi_ngắn_hạn` as trai_phieu_chuyen_doi_ngan_han,
            `trái_phiếu_phát_hành_ngắn_hạn` as trai_phieu_phat_hanh_ngan_han,
            `vay_quỹ_hỗ_trợ_thanh_toán` as vay_quy_ho_tro_thanh_toan,
            `phải_trả_hoạt_động_giao_dịch_chứng_khoán`
            as phai_tra_hoat_dong_giao_dich_chung_khoan,
            `phải_trả_về_lỗi_giao_dịch_các_tài_sản_tài_chính`
            as phai_tra_ve_loi_giao_dich_cac_tai_san_tai_chinh,
            `các_khoản_phải_trả_về_thuế` as cac_khoan_phai_tra_ve_thue,
            `các_khoản_trích_nộp_phúc_lợi_nhân_viên`
            as cac_khoan_trich_nop_phuc_loi_nhan_vien,
            `nhận_ký_quỹ_ký_cược_ngắn_hạn` as nhan_ky_quy_ky_cuoc_ngan_han,
            `phải_trả_về_xây_dựng_cơ_bản` as phai_tra_ve_xay_dung_co_ban,
            `phải_trả_cổ_tức_gốc_và_lãi_trái_phiếu`
            as phai_tra_co_tuc_goc_va_lai_trai_phieu,
            `phải_trả_tổ_chức_phát_hành_chứng_khoán`
            as phai_tra_to_chuc_phat_hanh_chung_khoan,
            `vay_tài_sản_tài_chính_dài_hạn` as vay_tai_san_tai_chinh_dai_han,
            `trái_phiếu_chuyển_đổi_dài_hạn` as trai_phieu_chuyen_doi_dai_han,
            `trái_phiếu_phát_hành_dài_hạn` as trai_phieu_phat_hanh_dai_han,
            `ký_quỹ_ký_cược_dài_hạn` as ky_quy_ky_cuoc_dai_han,
            `quỹ_bảo_vệ_nhà_đầu_tư` as quy_bao_ve_nha_dau_tu,
            `quỹ_dự_trữ_điều_lệ` as quy_du_tru_dieu_le,
            `quỹ_dự_phòng_tài_chính` as quy_du_phong_tai_chinh,
            `phải_thu_về_hợp_đồng_bảo_hiểm` as phai_thu_ve_hop_dong_bao_hiem,
            `phải_thu_khác_của_khách_hàng` as phai_thu_khac_cua_khach_hang,
            `tạm_ứng` as tam_ung,
            `phải_thu_từ_hoạt_động_đầu_tư_tài_chính`
            as phai_thu_tu_hoat_dong_dau_tu_tai_chinh,
            `chi_phí_hoa_hồng_chưa_phân_bổ` as chi_phi_hoa_hong_chua_phan_bo,
            `chi_phí_trả_trước_ngắn_hạn_khác` as chi_phi_tra_truoc_ngan_han_khac,
            `tài_sản_tái_bảo_hiểm` as tai_san_tai_bao_hiem,
            `dự_phòng_phí_nhượng_tái_bảo_hiểm` as du_phong_phi_nhuong_tai_bao_hiem,
            `dự_phòng_bồi_thường_nhượng_tái_bảo_hiểm`
            as du_phong_boi_thuong_nhuong_tai_bao_hiem,
            `ký_quỹ_bảo_hiểm` as ky_quy_bao_hiem,
            `tài_sản_ký_quỹ_dài_hạn` as tai_san_ky_quy_dai_han,
            `vay_và_nợ_ngắn_hạn` as vay_va_no_ngan_han,
            `phải_trả_thương_mại` as phai_tra_thuong_mai,
            `phải_trả_về_hợp_đồng_bảo_hiểm` as phai_tra_ve_hop_dong_bao_hiem,
            `phải_trả_khác_cho_người_bán` as phai_tra_khac_cho_nguoi_ban,
            `doanh_thu_hoa_hồng_chưa_được_hưởng` as doanh_thu_hoa_hong_chua_duoc_huong,
            `dự_phòng_nghiệp_vụ_bảo_hiểm` as du_phong_nghiep_vu_bao_hiem,
            `dự_phòng_phí_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`
            as du_phong_phi_bao_hiem_goc_va_nhan_tai_bao_hiem,
            `dự_phòng_toán_học` as du_phong_toan_hoc,
            `dự_phòng_bồi_thường_bảo_hiểm_gốc_nhận_tái_bảo_hiểm`
            as du_phong_boi_thuong_bao_hiem_goc_nhan_tai_bao_hiem,
            `dự_phòng_dao_động_lớn` as du_phong_dao_dong_lon,
            `dự_phòng_chia_lãi` as du_phong_chia_lai,
            `dự_phòng_đảm_bảo_cân_đối` as du_phong_dam_bao_can_doi,
            `quỹ_dự_trữ_bắt_buộc` as quy_du_tru_bat_buoc,

            --
            coalesce(tong_no_phai_tra, no_phai_tra, 0) as total_liabilities,

            coalesce(trai_phieu_chuyen_doi, 0)
            + coalesce(trai_phieu_chuyen_doi_dai_han, 0)
            + coalesce(trai_phieu_phat_hanh_dai_han, 0)
            + coalesce(co_phieu_uu_dai, 0)
            + coalesce(co_phieu_uu_dai_no, 0)
            + coalesce(vay_va_no_thue_tai_chinh_dai_han, 0)
            + coalesce(vay_tai_san_tai_chinh_dai_han, 0) as long_term_debt,

            coalesce(vay_va_no_thue_tai_chinh_ngan_han, 0)
            + coalesce(vay_va_no_ngan_han, 0)
            + coalesce(trai_phieu_chuyen_doi_ngan_han, 0)
            + coalesce(trai_phieu_phat_hanh_ngan_han, 0)
            + coalesce(vay_tai_san_tai_chinh_ngan_han, 0) as short_term_debt

        from source
    )

select *
from renamed
