with
    source as (select * from {{ source("simplize", "fct_cf") }}),

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
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_kinh_doanh`
            as luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh,
            `lợi_nhuận_trước_thuế` as loi_nhuan_truoc_thue,
            `điều_chỉnh_cho_các_khoản:` as dieu_chinh_cho_cac_khoan,
            `khấu_hao_tài_sản_cố_định` as khau_hao_tai_san_co_dinh,
            `phân_bổ_lợi_thế_thương_mại` as phan_bo_loi_the_thuong_mai,
            `các_khoản_dự_phòng` as cac_khoan_du_phong,
            `lãi/lỗ_chênh_lệch_tỷ_giá_hối_đoái` as lai_lo_chenh_lech_ty_gia_hoi_doai,
            `lãi/lỗ_từ_thanh_lý_tài_sản_cố_định` as lai_lo_tu_thanh_ly_tai_san_co_dinh,
            `lãi/lỗ_từ_hoạt_động_đầu_tư` as lai_lo_tu_hoat_dong_dau_tu,
            `chi_phí_lãi_vay` as chi_phi_lai_vay,
            `thu_lãi_và_cổ_tức` as thu_lai_va_co_tuc,
            `các_khoản_điều_chỉnh_khác` as cac_khoan_dieu_chinh_khac,
            `lợi_nhuận_từ_hoạt_động_kinh_doanh_trước_thay_đổi_vốn_lưu_động`
            as loi_nhuan_tu_hoat_dong_kinh_doanh_truoc_thay_doi_von_luu_dong,
            `tăng/giảm_các_khoản_phải_thu` as tang_giam_cac_khoan_phai_thu,
            `tăng/giảm_hàng_tồn_kho` as tang_giam_hang_ton_kho,
            `tăng/giảm_các_khoản_phải_trả_không_kể_lãi_vay_phải_trả_thuế_thu_nhập_phải_nộp`
            as tang_giam_cac_khoan_phai_tra_khong_ke_lai_vay_phai_tra_thue_thu_nhap_phai_nop,
            `tăng/giảm_chi_phí_trả_trước` as tang_giam_chi_phi_tra_truoc,
            `tăng/giảm_chứng_khoán_kinh_doanh` as tang_giam_chung_khoan_kinh_doanh,
            `thuế_thu_nhập_doanh_nghiệp_đã_nộp` as thue_thu_nhap_doanh_nghiep_da_nop,
            `lãi_vay_đã_trả` as lai_vay_da_tra,
            `tiền_thu_khác_từ_hoạt_động_kinh_doanh`
            as tien_thu_khac_tu_hoat_dong_kinh_doanh,
            `tiền_chi_khác_từ_hoạt_động_kinh_doanh`
            as tien_chi_khac_tu_hoat_dong_kinh_doanh,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_đầu_tư`
            as luu_chuyen_tien_thuan_tu_hoat_dong_dau_tu,
            `tiền_chi_mua_sắm_xây_dựng_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as tien_chi_mua_sam_xay_dung_tai_san_co_dinh_va_cac_tai_san_dai_han_khac,
            `tiền_thu_từ_thanh_lý_nhượng_bán_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as tien_thu_tu_thanh_ly_nhuong_ban_tai_san_co_dinh_va_cac_tai_san_dai_han_khac,
            `tiền_chi_cho_vay_mua_các_công_cụ_nợ_của_đơn_vị_khác`
            as tien_chi_cho_vay_mua_cac_cong_cu_no_cua_don_vi_khac,
            `tiền_thu_hồi_cho_vay_bán_lại_các_công_cụ_nợ_của_đơn_vị_khác`
            as tien_thu_hoi_cho_vay_ban_lai_cac_cong_cu_no_cua_don_vi_khac,
            `tiền_chi_đầu_tư_góp_vốn_vào_đơn_vị_khác`
            as tien_chi_dau_tu_gop_von_vao_don_vi_khac,
            `tiền_thu_hồi_đầu_tư_góp_vốn_vào_đơn_vị_khác`
            as tien_thu_hoi_dau_tu_gop_von_vao_don_vi_khac,
            `tiền_thu_lãi_cho_vay_cổ_tức_và_lợi_nhuận_được_chia`
            as tien_thu_lai_cho_vay_co_tuc_va_loi_nhuan_duoc_chia,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_tài_chính`
            as luu_chuyen_tien_thuan_tu_hoat_dong_tai_chinh,
            `tiền_thu_phát_hành_cổ_phiếu_nhận_vốn_góp_của_chủ_sở_hữu`
            as tien_thu_phat_hanh_co_phieu_nhan_von_gop_cua_chu_so_huu,
            `tiền_chi_trả_vốn_góp_cho_các_chủ_sở_hữu_mua_lại_cổ_phiếu_của_doanh_nghiệp_đã_phát_hành`
            as tien_chi_tra_von_gop_cho_cac_chu_so_huu_mua_lai_co_phieu_cua_doanh_nghiep_da_phat_hanh,
            `tiền_vay_ngắn_dài_hạn_được_nhận` as tien_vay_ngan_dai_han_duoc_nhan,
            `tiền_chi_trả_nợ_gốc_vay` as tien_chi_tra_no_goc_vay,
            `tiền_chi_trả_nợ_thuê_tài_chính` as tien_chi_tra_no_thue_tai_chinh,
            `cổ_tức_lợi_nhuận_đã_trả_cho_chủ_sở_hữu`
            as co_tuc_loi_nhuan_da_tra_cho_chu_so_huu,
            `lưu_chuyển_tiền_thuần_trong_kỳ` as luu_chuyen_tien_thuan_trong_ky,
            `tiền_và_tương_đương_tiền_đầu_kỳ` as tien_va_tuong_duong_tien_dau_ky,
            `ảnh_hưởng_của_thay_đổi_tỷ_giá_hối_đoái_quy_đổi_ngoại_tệ`
            as anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te,
            `tiền_và_tương_đương_tiền_cuối_kỳ` as tien_va_tuong_duong_tien_cuoi_ky,
            `thu_nhập_lãi_và_các_khoản_thu_nhập_tương_tự_nhận_được`
            as thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu_nhan_duoc,
            `chi_phí_lãi_và_các_chi_phí_tương_tự_đã_trả`
            as chi_phi_lai_va_cac_chi_phi_tuong_tu_da_tra,
            `thu_nhập_từ_hoạt_động_dịch_vụ_nhận_được`
            as thu_nhap_tu_hoat_dong_dich_vu_nhan_duoc,
            `chênh_lệch_số_tiền_thực_thu/thực_chi_từ_hoạt_động_kinh_doanh_ngoại_tệ_vàng_bạc_chứng_khoán`
            as chenh_lech_so_tien_thuc_thu_thuc_chi_tu_hoat_dong_kinh_doanh_ngoai_te_vang_bac_chung_khoan,
            `thu_nhập_khác` as thu_nhap_khac,
            `tiền_thu_các_khoản_nợ_đã_được_xử_lý_hóa_bù_đắp_bằng_nguồn_rủi_ro`
            as tien_thu_cac_khoan_no_da_duoc_xu_ly_hoa_bu_dap_bang_nguon_rui_ro,
            `tiền_chi_trả_cho_nhân_viên_và_hoạt_động_quản_lý_công_vụ`
            as tien_chi_tra_cho_nhan_vien_va_hoat_dong_quan_ly_cong_vu,
            `tiền_thuế_thu_nhập_thực_nộp_trong_kỳ`
            as tien_thue_thu_nhap_thuc_nop_trong_ky,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_kinh_doanh_trước_những_thay_đổi_về_tài_sản_và_vốn_lưu_động`
            as luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh_truoc_nhung_thay_doi_ve_tai_san_va_von_luu_dong,
            `những_thay_đổi_về_tài_sản_hoạt_động:`
            as nhung_thay_doi_ve_tai_san_hoat_dong,
            `tăng/giảm_các_khoản_tiền_vàng_gửi_và_cho_vay_các_tổ_chức_tín_dụng_khác`
            as tang_giam_cac_khoan_tien_vang_gui_va_cho_vay_cac_to_chuc_tin_dung_khac,
            `tăng/giảm_các_khoản_về_kinh_doanh_chứng_khoán`
            as tang_giam_cac_khoan_ve_kinh_doanh_chung_khoan,
            `tăng/giảm_các_công_cụ_tài_chính_phái_sinh_và_các_tài_sản_tài_chính_khác`
            as tang_giam_cac_cong_cu_tai_chinh_phai_sinh_va_cac_tai_san_tai_chinh_khac,
            `tăng/giảm_các_khoản_cho_vay_khách_hàng`
            as tang_giam_cac_khoan_cho_vay_khach_hang,
            `giảm_nguồn_dự_phòng_để_bù_đắp_tổn_thất_các_khoản`
            as giam_nguon_du_phong_de_bu_dap_ton_that_cac_khoan,
            `tăng/giảm_khác_về_tài_sản_hoạt_động`
            as tang_giam_khac_ve_tai_san_hoat_dong,
            `những_thay_đổi_về_công_nợ_hoạt_động:`
            as nhung_thay_doi_ve_cong_no_hoat_dong,
            `tăng/giảm_các_khoản_nợ_chính_phủ_và_ngân_hàng_nhà_nước`
            as tang_giam_cac_khoan_no_chinh_phu_va_ngan_hang_nha_nuoc,
            `tăng/giảm_các_khoản_tiền_gửi_tiền_vay_các_tổ_chức_tín_dụng`
            as tang_giam_cac_khoan_tien_gui_tien_vay_cac_to_chuc_tin_dung,
            `tăng/giảm_tiền_gửi_của_khách_hàng_bao_gồm_cả_kho_bạc_nhà_nước`
            as tang_giam_tien_gui_cua_khach_hang_bao_gom_ca_kho_bac_nha_nuoc,
            `tăng/giảm_phát_hành_giấy_tờ_có_giá_ngoại_trừ_giấy_tờ_có_giá_phát_hành_được_tính_vào_hoạt_động_tài_chính`
            as tang_giam_phat_hanh_giay_to_co_gia_ngoai_tru_giay_to_co_gia_phat_hanh_duoc_tinh_vao_hoat_dong_tai_chinh,
            `tăng/giảm_vốn_tài_trợ_ủy_thác_đầu_tư_cho_vay_mà_tổ_chức_tín_dụng_chịu_rủi_ro`
            as tang_giam_von_tai_tro_uy_thac_dau_tu_cho_vay_ma_to_chuc_tin_dung_chiu_rui_ro,
            `tăng/giảm_các_công_cụ_tài_chính_phái_sinh_và_các_khoản_nợ_tài_chính_khác`
            as tang_giam_cac_cong_cu_tai_chinh_phai_sinh_va_cac_khoan_no_tai_chinh_khac,
            `tăng/giảm_lãi_phí_phải_trả` as tang_giam_lai_phi_phai_tra,
            `tăng/giảm_khác_về_công_nợ_hoạt_động`
            as tang_giam_khac_ve_cong_no_hoat_dong,
            `chi_từ_các_quỹ_của_tổ_chức_tín_dụng`
            as chi_tu_cac_quy_cua_to_chuc_tin_dung,
            `tiền_chi_từ_thanh_lý_nhượng_bán_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as tien_chi_tu_thanh_ly_nhuong_ban_tai_san_co_dinh_va_cac_tai_san_dai_han_khac,
            `mua_sắm_bất_động_sản_đầu_tư` as mua_sam_bat_dong_san_dau_tu,
            `tiền_thu_từ_bán_thanh_lý_bất_động_sản_đầu_tư`
            as tien_thu_tu_ban_thanh_ly_bat_dong_san_dau_tu,
            `tiền_chi_ra_do_bán_thanh_lý_bất_động_sản_đầu_tư`
            as tien_chi_ra_do_ban_thanh_ly_bat_dong_san_dau_tu,
            `tiền_thu_từ_phát_hành_giấy_tờ_có_giá_dài_hạn_có_đủ_điều_kiện_tính_vào_vốn_tự_có_và_các_khoản_vốn_vay_dài_hạn_khác`
            as tien_thu_tu_phat_hanh_giay_to_co_gia_dai_han_co_du_dieu_kien_tinh_vao_von_tu_co_va_cac_khoan_von_vay_dai_han_khac,
            `tiền_chi_thanh_toán_giấy_tờ_có_giá_dài_hạn_đủ_điều_kiện_tính_vào_vốn_tự_có_và_các_khoản_vốn_vay_dài_hạn_khác`
            as tien_chi_thanh_toan_giay_to_co_gia_dai_han_du_dieu_kien_tinh_vao_von_tu_co_va_cac_khoan_von_vay_dai_han_khac,
            `tiền_chi_ra_mua_cổ_phiếu_quỹ` as tien_chi_ra_mua_co_phieu_quy,
            `tiền_thu_được_do_bán_cổ_phiếu_quỹ` as tien_thu_duoc_do_ban_co_phieu_quy,
            `lãi/lỗ_từ_hoạt_động_đầu_tư_đầu_tư_công_ty_con_liên_doanh_liên_kết`
            as lai_lo_tu_hoat_dong_dau_tu_dau_tu_cong_ty_con_lien_doanh_lien_ket,
            `tiền_lãi_dự_thu` as tien_lai_du_thu,
            `tăng_các_chi_phí_phi_tiền_tệ:` as tang_cac_chi_phi_phi_tien_te,
            `lỗ_đánh_giá_lại_giá_trị_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as lo_danh_gia_lai_gia_tri_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `lỗ_suy_giảm_giá_trị_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`
            as lo_suy_giam_gia_tri_cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            `lỗ_suy_giảm_giá_trị_các_khoản_cho_vay`
            as lo_suy_giam_gia_tri_cac_khoan_cho_vay,
            `lỗ_về_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs_khi_phân_loại_lại`
            as lo_ve_ghi_nhan_chenh_lech_danh_gia_theo_gia_tri_hop_ly_tai_san_tai_chinh_san_sang_de_ban_afs_khi_phan_loai_lai,
            `suy_giảm_giá_trị_của_các_tài_sản_cố_định_bất_động_sản_đầu_tư`
            as suy_giam_gia_tri_cua_cac_tai_san_co_dinh_bat_dong_san_dau_tu,
            `chi_phí_dự_phòng_suy_giảm_giá_trị_các_khoản_đầu_tư_tài_chính_dài_hạn`
            as chi_phi_du_phong_suy_giam_gia_tri_cac_khoan_dau_tu_tai_chinh_dai_han,
            `lỗ_khác` as lo_khac,
            `giảm_các_doanh_thu_phi_tiền_tệ:` as giam_cac_doanh_thu_phi_tien_te,
            `lãi_đánh_giá_lại_giá_trị_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as lai_danh_gia_lai_gia_tri_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `lãi_về_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs_khi_phân_loại_lại`
            as lai_ve_ghi_nhan_chenh_lech_danh_gia_theo_gia_tri_hop_ly_tai_san_tai_chinh_san_sang_de_ban_afs_khi_phan_loai_lai,
            `lãi_khác` as lai_khac,
            `thay_đổi_tài_sản_và_nợ_phải_trả_hoạt_động:`
            as thay_doi_tai_san_va_no_phai_tra_hoat_dong,
            `tăng/giảm_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as tang_giam_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `tăng/giảm_các_khoản_đầu_tư_nắm_giữa_đến_ngày_đáo_hạn_htm`
            as tang_giam_cac_khoan_dau_tu_nam_giua_den_ngay_dao_han_htm,
            `tăng/giảm_các_khoản_cho_vay` as tang_giam_cac_khoan_cho_vay,
            `tăng/giảm_tài_sản_tài_chính_sẵn_sàng_để_bán_afs`
            as tang_giam_tai_san_tai_chinh_san_sang_de_ban_afs,
            `tăng/giảm_phải_thu_bán_các_tài_sản_tài_chính`
            as tang_giam_phai_thu_ban_cac_tai_san_tai_chinh,
            `tăng/giảm_phải_thu_và_dự_thu_cổ_tức_tiền_lãi_các_tài_sản_tài_chính`
            as tang_giam_phai_thu_va_du_thu_co_tuc_tien_lai_cac_tai_san_tai_chinh,
            `tăng/giảm_các_khoản_phải_thu_các_dịch_vụ_công_ty_chứng_khoán_cung_cấp`
            as tang_giam_cac_khoan_phai_thu_cac_dich_vu_cong_ty_chung_khoan_cung_cap,
            `tăng/giảm_các_khoản_phải_thu_về_lỗi_giao_dịch_các_tài_sản_tài_chính`
            as tang_giam_cac_khoan_phai_thu_ve_loi_giao_dich_cac_tai_san_tai_chinh,
            `tăng/giảm_các_khoản_phải_thu_khác` as tang_giam_cac_khoan_phai_thu_khac,
            `tăng/giảm_các_tài_sản_khác` as tang_giam_cac_tai_san_khac,
            `tăng/giảm_chi_phí_phải_trả_không_bao_gồm_chi_phí_lãi_vay`
            as tang_giam_chi_phi_phai_tra_khong_bao_gom_chi_phi_lai_vay,
            `tăng/giảm_phải_trả_cho_người_bán` as tang_giam_phai_tra_cho_nguoi_ban,
            `tăng/giảm_các_khoản_trích_nộp_phúc_lợi_nhân_viên`
            as tang_giam_cac_khoan_trich_nop_phuc_loi_nhan_vien,
            `tăng/giảm_thuế_và_các_khoản_phải_nộp_nhà_nước_không_bao_gồm_thuế_thu_nhập_doanh_nghiệp_đã_nộp`
            as tang_giam_thue_va_cac_khoan_phai_nop_nha_nuoc_khong_bao_gom_thue_thu_nhap_doanh_nghiep_da_nop,
            `tăng/giảm_phải_trả_người_lao_động` as tang_giam_phai_tra_nguoi_lao_dong,
            `tăng/giảm_phải_trả_về_lỗi_giao_dịch_các_tài_sản_tài_chính`
            as tang_giam_phai_tra_ve_loi_giao_dich_cac_tai_san_tai_chinh,
            `tăng/giảm_phải_trả_phải_nộp_khác` as tang_giam_phai_tra_phai_nop_khac

        from source
    )

select *
from renamed
