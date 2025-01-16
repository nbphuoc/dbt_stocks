with
    source as (select * from {{ source("simplize", "fct_pnl") }}),

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
            `doanh_thu_bán_hàng_và_cung_cấp_dịch_vụ`
            as doanh_thu_ban_hang_va_cung_cap_dich_vu,
            `các_khoản_giảm_trừ_doanh_thu` as cac_khoan_giam_tru_doanh_thu,
            `doanh_thu_thuần_về_bán_hàng_và_cung_cấp_dịch_vụ`
            as doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu,
            `giá_vốn_hàng_bán` as gia_von_hang_ban,
            `lợi_nhuận_gộp_về_bán_hàng_và_cung_cấp_dịch_vụ`
            as loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu,
            `doanh_thu_hoạt_động_tài_chính` as doanh_thu_hoat_dong_tai_chinh,
            `chi_phí_tài_chính` as chi_phi_tai_chinh,
            `chi_phí_lãi_vay` as chi_phi_lai_vay,
            `lợi_nhuận_từ_công_ty_liên_doanh_liên_kết`
            as loi_nhuan_tu_cong_ty_lien_doanh_lien_ket,
            `chi_phí_bán_hàng` as chi_phi_ban_hang,
            `chi_phí_quản_lý_doanh_nghiệp` as chi_phi_quan_ly_doanh_nghiep,
            `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh`
            as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh,
            `lợi_nhuận_khác` as loi_nhuan_khac,
            `thu_nhập_khác` as thu_nhap_khac,
            `chi_phí_khác` as chi_phi_khac,
            `lợi_nhuận_kế_toán_trước_thuế` as loi_nhuan_ke_toan_truoc_thue,
            `thuế_thu_nhập_doanh_nghiệp` as thue_thu_nhap_doanh_nghiep,
            `chi_phí_thuế_thu_nhập_hiện_hành` as chi_phi_thue_thu_nhap_hien_hanh,
            `chi_phí_thuế_thu_nhập_hoãn_lại` as chi_phi_thue_thu_nhap_hoan_lai,
            `lợi_nhuận_kế_toán_sau_thuế` as loi_nhuan_ke_toan_sau_thue,
            `lợi_nhuận_cổ_đông_công_ty_mẹ` as loi_nhuan_co_dong_cong_ty_me,
            `lợi_nhuận_cổ_đông_thiểu_số` as loi_nhuan_co_dong_thieu_so,
            `thu_nhập_lãi_thuần` as thu_nhap_lai_thuan,
            `thu_nhập_lãi_và_các_khoản_thu_nhập_tương_tự`
            as thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu,
            `chi_phí_lãi_và_các_chi_phí_tương_tự`
            as chi_phi_lai_va_cac_chi_phi_tuong_tu,
            `lãi/lỗ_thuần_từ_hoạt_động_dịch_vụ` as lai_lo_thuan_tu_hoat_dong_dich_vu,
            `thu_nhập_từ_hoạt_động_dịch_vụ` as thu_nhap_tu_hoat_dong_dich_vu,
            `chi_phí_hoạt_động_dịch_vụ` as chi_phi_hoat_dong_dich_vu,
            `lãi/lỗ_thuần_từ_hoạt_động_kinh_doanh_ngoại_hối_và_vàng`
            as lai_lo_thuan_tu_hoat_dong_kinh_doanh_ngoai_hoi_va_vang,
            `lãi/lỗ_thuần_từ_mua_bán_chứng_khoán_kinh_doanh`
            as lai_lo_thuan_tu_mua_ban_chung_khoan_kinh_doanh,
            `lãi/lỗ_thuần_từ_mua_bán_chứng_khoán_đầu_tư`
            as lai_lo_thuan_tu_mua_ban_chung_khoan_dau_tu,
            `lãi/lỗ_thuần_từ_hoạt_động_khác` as lai_lo_thuan_tu_hoat_dong_khac,
            `thu_nhập_từ_hoạt_động_khác` as thu_nhap_tu_hoat_dong_khac,
            `chi_phí_hoạt_động_khác` as chi_phi_hoat_dong_khac,
            `thu_nhập_từ_góp_vốn_mua_cổ_phần` as thu_nhap_tu_gop_von_mua_co_phan,
            `tổng_thu_nhập_hoạt_động` as tong_thu_nhap_hoat_dong,
            `chi_phí_hoạt_động` as chi_phi_hoat_dong,
            `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh_trước_chi_phí_dự_phòng_rủi_ro_tín_dụng`
            as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung,
            `chi_phí_dự_phòng_rủi_ro_tín_dụng` as chi_phi_du_phong_rui_ro_tin_dung,
            `doanh_thu_hoạt_động` as doanh_thu_hoat_dong,
            `lãi_từ_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as lai_tu_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `lãi_bán_các_tài_sản_tài_chính` as lai_ban_cac_tai_san_tai_chinh,
            `chênh_lệch_tăng_đánh_giá_lại_các_tài_sản_tài_chính_thông_qua_lãi/lỗ`
            as chenh_lech_tang_danh_gia_lai_cac_tai_san_tai_chinh_thong_qua_lai_lo,
            `cổ_tức_tiền_lãi_phát_sinh_từ_tài_sản_tài_chính_fvtpl`
            as co_tuc_tien_lai_phat_sinh_tu_tai_san_tai_chinh_fvtpl,
            `lãi_từ_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`
            as lai_tu_cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            `lãi_từ_các_khoản_cho_vay_và_phải_thu`
            as lai_tu_cac_khoan_cho_vay_va_phai_thu,
            `lãi_từ_các_tài_sản_tài_chính_sẵn_sàng_để_bán_afs`
            as lai_tu_cac_tai_san_tai_chinh_san_sang_de_ban_afs,
            `lãi_từ_các_công_cụ_phát_sinh_phòng_ngừa_rủi_ro`
            as lai_tu_cac_cong_cu_phat_sinh_phong_ngua_rui_ro,
            `doanh_thu_từ_hoạt_động_môi_giới_chứng_khoán`
            as doanh_thu_tu_hoat_dong_moi_gioi_chung_khoan,
            `doanh_thu_bảo_lãnh_phát_hành_chứng_khoán`
            as doanh_thu_bao_lanh_phat_hanh_chung_khoan,
            `doanh_thu_đại_lý_phát_hành_chứng_khoán`
            as doanh_thu_dai_ly_phat_hanh_chung_khoan,
            `doanh_thu_hoạt_động_tư_vấn_đầu_tư_chứng_khoán`
            as doanh_thu_hoat_dong_tu_van_dau_tu_chung_khoan,
            `doanh_thu_hoạt_động_ủy_thác_đấu_giá`
            as doanh_thu_hoat_dong_uy_thac_dau_gia,
            `doanh_thu_lưu_ký_chứng_khoán` as doanh_thu_luu_ky_chung_khoan,
            `doanh_thu_hoạt_động_đầu_tư_chứng_khoán_góp_vốn`
            as doanh_thu_hoat_dong_dau_tu_chung_khoan_gop_von,
            `thu_cho_thuê_sử_dụng_tài_sản` as thu_cho_thue_su_dung_tai_san,
            `doanh_thu_hoạt_động_tư_vấn_tài_chính`
            as doanh_thu_hoat_dong_tu_van_tai_chinh,
            `doanh_thu_khác` as doanh_thu_khac,
            `doanh_thu_thuần` as doanh_thu_thuan,
            `chi_phí_hoạt_động_kinh_doanh` as chi_phi_hoat_dong_kinh_doanh,
            `lỗ_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi_lỗ_fvtpl`
            as lo_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            `lỗ_bán_các_tài_sản_tài_chính` as lo_ban_cac_tai_san_tai_chinh,
            `chênh_lệch_giảm_đánh_giá_lại_các_tài_sản_tài_chính_fvtpl`
            as chenh_lech_giam_danh_gia_lai_cac_tai_san_tai_chinh_fvtpl,
            `chi_phí_giao_dịch_mua_các_tài_sản_tài_chính_fvtpl`
            as chi_phi_giao_dich_mua_cac_tai_san_tai_chinh_fvtpl,
            `lỗ_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`
            as lo_cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            `chi_phí_lãi_vay_lỗ_từ_các_khoản_cho_vay_và_phải_thu`
            as chi_phi_lai_vay_lo_tu_cac_khoan_cho_vay_va_phai_thu,
            `lỗ_và_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs`
            as lo_va_ghi_nhan_chenh_lech_danh_gia_theo_gia_tri_hop_ly_tai_san_tai_chinh_san_sang_de_ban_afs,
            `chi_phí_dự_phòng_tài_sản_tài_chính_xử_lý_tổn_thất_các_khoản_phải_thu_khó_đòi_và_lỗ_suy_giảm_tài_sản_tài_chính_và_cổ_phiếu_đi_vay`
            as chi_phi_du_phong_tai_san_tai_chinh_xu_ly_ton_that_cac_khoan_phai_thu_kho_doi_va_lo_suy_giam_tai_san_tai_chinh_va_co_phieu_di_vay,
            `lỗ_từ_các_tài_sản_tài_chính_phái_sinh_phòng_ngừa_rủi_ro`
            as lo_tu_cac_tai_san_tai_chinh_phai_sinh_phong_ngua_rui_ro,
            `chi_phí_hoạt_động_tự_doanh` as chi_phi_hoat_dong_tu_doanh,
            `chi_phí_môi_giới_chứng_khoán` as chi_phi_moi_gioi_chung_khoan,
            `chi_phí_hoạt_động_bảo_lãnh_đại_lý_phát_hành_chứng_khoán`
            as chi_phi_hoat_dong_bao_lanh_dai_ly_phat_hanh_chung_khoan,
            `chi_phí_tư_vấn_đầu_tư_chứng_khoán` as chi_phi_tu_van_dau_tu_chung_khoan,
            `chi_phí_hoạt_động_đấu_giá_ủy_thác` as chi_phi_hoat_dong_dau_gia_uy_thac,
            `chi_phí_lưu_ký_chứng_khoán` as chi_phi_luu_ky_chung_khoan,
            `chi_phí_hoạt_động_tư_vấn_tài_chính` as chi_phi_hoat_dong_tu_van_tai_chinh,
            `chi_phí_các_dịch_vụ_khác` as chi_phi_cac_dich_vu_khac,
            `lợi_nhuận_gộp` as loi_nhuan_gop,
            `chênh_lệch_lãi_tỷ_giá_hối_đoái_đã_và_chưa_thực_hiện`
            as chenh_lech_lai_ty_gia_hoi_doai_da_va_chua_thuc_hien,
            `doanh_thu_dự_thu_cổ_tức_lãi_tiền_gửi_không_cố_định_phát_sinh_trong_kỳ`
            as doanh_thu_du_thu_co_tuc_lai_tien_gui_khong_co_dinh_phat_sinh_trong_ky,
            `lãi_bán_thanh_lý_các_khoản_đầu_tư_vào_công_ty_con_liên_kết_liên_doanh`
            as lai_ban_thanh_ly_cac_khoan_dau_tu_vao_cong_ty_con_lien_ket_lien_doanh,
            `doanh_thu_khác_về_đầu_tư` as doanh_thu_khac_ve_dau_tu,
            `chênh_lệch_lỗ_tỷ_giá_hối_đoái_đã_và_chưa_thực_hiện`
            as chenh_lech_lo_ty_gia_hoi_doai_da_va_chua_thuc_hien,
            `lỗ_bán_thanh_lý_các_khoản_đầu_tư_vào_công_ty_con_liên_kết_liên_doanh`
            as lo_ban_thanh_ly_cac_khoan_dau_tu_vao_cong_ty_con_lien_ket_lien_doanh,
            `chi_phí_dự_phòng_suy_giảm_giá_trị_các_khoản_đầu_tư_tài_chính_dài_hạn`
            as chi_phi_du_phong_suy_giam_gia_tri_cac_khoan_dau_tu_tai_chinh_dai_han,
            `chi_phí_tài_chính_khác` as chi_phi_tai_chinh_khac,
            `chi_phí_quản_lý_công_ty_chứng_khoán`
            as chi_phi_quan_ly_cong_ty_chung_khoan,
            `kết_quả_hoạt_động` as ket_qua_hoat_dong,
            `doanh_thu_phí_bảo_hiểm` as doanh_thu_phi_bao_hiem,
            `phí_bảo_hiểm_gốc` as phi_bao_hiem_goc,
            `phí_nhận_tái_bảo_hiểm` as phi_nhan_tai_bao_hiem,
            `tăng_dự_phòng_phí_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`
            as tang_du_phong_phi_bao_hiem_goc_va_nhan_tai_bao_hiem,
            `phí_nhượng_tái_bảo_hiểm` as phi_nhuong_tai_bao_hiem,
            `tổng_phí_nhượng_tái_bảo_hiểm` as tong_phi_nhuong_tai_bao_hiem,
            `tăng_dự_phòng_phí_nhượng_tái_bảo_hiểm`
            as tang_du_phong_phi_nhuong_tai_bao_hiem,
            `doanh_thu_phí_bảo_hiểm_thuần` as doanh_thu_phi_bao_hiem_thuan,
            `hoa_hồng_nhượng_tái_bảo_hiểm_và_doanh_thu_khác_hoạt_động_kinh_doanh_bảo_hiểm`
            as hoa_hong_nhuong_tai_bao_hiem_va_doanh_thu_khac_hoat_dong_kinh_doanh_bao_hiem,
            `hoa_hồng_nhượng_tái_bảo_hiểm` as hoa_hong_nhuong_tai_bao_hiem,
            `doanh_thu_khác_hoạt_động_kinh_doanh_bảo_hiểm`
            as doanh_thu_khac_hoat_dong_kinh_doanh_bao_hiem,
            `doanh_thu_thuần_hoạt_động_kinh_doanh_bảo_hiểm`
            as doanh_thu_thuan_hoat_dong_kinh_doanh_bao_hiem,
            `chi_bồi_thường` as chi_boi_thuong,
            `tổng_chi_bồi_thường` as tong_chi_boi_thuong,
            `các_khoản_giảm_trừ` as cac_khoan_giam_tru,
            `thu_bồi_thường_nhượng_tái_bảo_hiểm` as thu_boi_thuong_nhuong_tai_bao_hiem,
            `tăng/giảm_dự_phòng_toán_học` as tang_giam_du_phong_toan_hoc,
            `tăng_dự_phòng_bồi_thường_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`
            as tang_du_phong_boi_thuong_bao_hiem_goc_va_nhan_tai_bao_hiem,
            `tăng_dự_phòng_bồi_thường_nhượng_tái_bảo_hiểm`
            as tang_du_phong_boi_thuong_nhuong_tai_bao_hiem,
            `tổng_chi_bồi_thường_bảo_hiểm` as tong_chi_boi_thuong_bao_hiem,
            `tăng_dự_phòng_dao_động_lớn` as tang_du_phong_dao_dong_lon,
            `sử_dụng_từ_dự_phòng_dao_động_lớn` as su_dung_tu_du_phong_dao_dong_lon,
            `chi_phí_khác_hoạt_động_bảo_hiểm` as chi_phi_khac_hoat_dong_bao_hiem,
            `chi_hoa_hồng_bảo_hiểm_gốc` as chi_hoa_hong_bao_hiem_goc,
            `chi_phí_khác_hoạt_động_bảo_hiểm_gốc`
            as chi_phi_khac_hoat_dong_bao_hiem_goc,
            `chi_khác_nhận_tái_bảo_hiểm_khác` as chi_khac_nhan_tai_bao_hiem_khac,
            `chi_nhượng_tái_bảo_hiểm` as chi_nhuong_tai_bao_hiem,
            `tổng_chi_phí_hoạt_động_kinh_doanh_bảo_hiểm`
            as tong_chi_phi_hoat_dong_kinh_doanh_bao_hiem,
            `lợi_nhuận_gộp_hoạt_động_kinh_doanh_bảo_hiểm`
            as loi_nhuan_gop_hoat_dong_kinh_doanh_bao_hiem,
            `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh_bảo_hiểm`
            as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_bao_hiem,
            `lợi_nhuận_hoạt_động_tài_chính` as loi_nhuan_hoat_dong_tai_chinh,
            `doanh_thu_tài_chính` as doanh_thu_tai_chinh,
            `lợi_nhuận_hoạt_động_đầu_tư_bất_động_sản`
            as loi_nhuan_hoat_dong_dau_tu_bat_dong_san,
            `lợi_nhuận_thuần_hoạt_động_ngân_hàng`
            as loi_nhuan_thuan_hoat_dong_ngan_hang,
            `lợi_nhuận_hoạt_động_khác` as loi_nhuan_hoat_dong_khac,
            `thu_nhập_khác_ròng` as thu_nhap_khac_rong
        from source
    )

select *
from renamed
