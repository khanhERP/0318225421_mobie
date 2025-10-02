
import React, { useState, useEffect } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ArrowLeft, Trash2, Edit, Save, X } from "lucide-react";
import { toast } from "@/hooks/use-toast";

interface IncomeVoucher {
  id?: string;
  voucherNumber: string;
  date: string;
  amount: number;
  account: string;
  recipient: string;
  phone: string;
  category: string;
  description: string;
}

interface IncomeVoucherModalProps {
  isOpen: boolean;
  onClose: () => void;
  voucher?: IncomeVoucher | null;
  mode: "create" | "edit";
}

const INCOME_CATEGORIES = [
  "Bán hàng",
  "Dịch vụ", 
  "Thu nợ",
  "Thu khác",
  "Hoàn tiền",
];

const CASH_ACCOUNTS = [
  "Tiền mặt",
  "Ngân hàng",
  "Ví điện tử",
];

export default function IncomeVoucherModal({ 
  isOpen, 
  onClose, 
  voucher, 
  mode 
}: IncomeVoucherModalProps) {
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [isEditing, setIsEditing] = useState(mode === "create");
  const queryClient = useQueryClient();
  
  const [formData, setFormData] = useState<IncomeVoucher>({
    voucherNumber: "",
    date: new Date().toISOString().split('T')[0],
    amount: 0,
    account: "Tiền mặt",
    recipient: "",
    phone: "",
    category: "Thu khác",
    description: "",
  });

  useEffect(() => {
    if (voucher && mode === "edit") {
      setFormData(voucher);
      setIsEditing(false);
    } else if (mode === "create") {
      // Generate voucher number for new voucher
      const today = new Date();
      const dateStr = today.toISOString().split('T')[0].replace(/-/g, '');
      const timeStr = Date.now().toString().slice(-3);
      setFormData(prev => ({
        ...prev,
        voucherNumber: `PT${dateStr}${timeStr}`,
      }));
      setIsEditing(true);
    }
  }, [voucher, mode]);

  const createVoucherMutation = useMutation({
    mutationFn: async (data: IncomeVoucher) => {
      const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!response.ok) throw new Error("Failed to create income voucher");
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Thành công",
        description: "Đã tạo phiếu thu mới",
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      onClose();
    },
    onError: (error) => {
      toast({
        title: "Lỗi",
        description: "Không thể tạo phiếu thu",
        variant: "destructive",
      });
    },
  });

  const updateVoucherMutation = useMutation({
    mutationFn: async (data: IncomeVoucher) => {
      console.log("Updating income voucher with data:", data);
      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers/${data.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        const errorMessage = errorData.error || `HTTP ${response.status}: ${response.statusText}`;
        throw new Error(errorMessage);
      }
      
      return response.json();
    },
    onSuccess: (data) => {
      console.log("Income voucher updated successfully:", data);
      toast({
        title: "Thành công", 
        description: `Đã cập nhật phiếu thu ${formData.voucherNumber} thành công`,
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      setIsEditing(false);
    },
    onError: (error) => {
      console.error("Failed to update income voucher:", error);
      const errorMessage = error instanceof Error ? error.message : "Không thể cập nhật phiếu thu";
      toast({
        title: "Lỗi cập nhật phiếu thu",
        description: errorMessage,
        variant: "destructive",
      });
    },
  });

  const deleteVoucherMutation = useMutation({
    mutationFn: async (id: string) => {
      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers/${id}`, {
        method: "DELETE",
      });
      if (!response.ok) throw new Error("Failed to delete income voucher");
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Thành công",
        description: "Đã xóa phiếu thu",
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      setShowDeleteDialog(false);
      onClose();
    },
    onError: (error) => {
      toast({
        title: "Lỗi", 
        description: "Không thể xóa phiếu thu",
        variant: "destructive",
      });
    },
  });

  const handleSave = () => {
    // Validate required fields
    if (!formData.voucherNumber?.trim()) {
      toast({
        title: "Lỗi",
        description: "Vui lòng nhập số phiếu thu",
        variant: "destructive",
      });
      return;
    }

    if (!formData.recipient?.trim()) {
      toast({
        title: "Lỗi",
        description: "Vui lòng nhập đối tượng nộp",
        variant: "destructive",
      });
      return;
    }

    if (!formData.amount || formData.amount <= 0) {
      toast({
        title: "Lỗi",
        description: "Vui lòng nhập số tiền hợp lệ (lớn hơn 0)",
        variant: "destructive",
      });
      return;
    }

    if (!formData.date?.trim()) {
      toast({
        title: "Lỗi",
        description: "Vui lòng chọn ngày thu",
        variant: "destructive",
      });
      return;
    }

    // Prepare clean data for submission
    const cleanData = {
      ...formData,
      id: voucher?.id, // Include ID for update
      voucherNumber: formData.voucherNumber.trim(),
      recipient: formData.recipient.trim(),
      account: formData.account || "Tiền mặt",
      category: formData.category || "Thu khác",
      date: formData.date.trim(),
      phone: formData.phone?.trim() || "",
      description: formData.description?.trim() || "",
      amount: Number(formData.amount),
    };

    console.log("Saving income voucher:", cleanData);

    if (mode === "create") {
      createVoucherMutation.mutate(cleanData);
    } else {
      updateVoucherMutation.mutate(cleanData);
    }
  };

  const handleDelete = () => {
    if (voucher?.id) {
      deleteVoucherMutation.mutate(voucher.id);
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('vi-VN').format(value);
  };

  return (
    <>
      <Dialog open={isOpen} onOpenChange={onClose}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-3">
              <Button variant="ghost" size="sm" onClick={onClose}>
                <ArrowLeft className="w-4 h-4" />
              </Button>
              <span>Phiếu thu</span>
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-6 p-4">
            {/* Form Fields */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Left Column */}
              <div className="space-y-4">
                <div>
                  <Label htmlFor="voucherNumber">Số phiếu thu *</Label>
                  <Input
                    id="voucherNumber"
                    value={formData.voucherNumber}
                    onChange={(e) => setFormData(prev => ({ ...prev, voucherNumber: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                  />
                </div>

                <div>
                  <Label htmlFor="date">Ngày thu *</Label>
                  <Input
                    id="date"
                    type="date"
                    value={formData.date}
                    onChange={(e) => setFormData(prev => ({ ...prev, date: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                  />
                </div>

                <div>
                  <Label htmlFor="amount">Số tiền *</Label>
                  <Input
                    id="amount"
                    type="number"
                    value={formData.amount || ""}
                    onChange={(e) => setFormData(prev => ({ ...prev, amount: parseFloat(e.target.value) || 0 }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                    placeholder="0"
                  />
                  {formData.amount > 0 && (
                    <p className="text-sm text-gray-600 mt-1">
                      {formatCurrency(formData.amount)} VND
                    </p>
                  )}
                </div>

                <div>
                  <Label htmlFor="account">Tài khoản thu *</Label>
                  <Select
                    value={formData.account}
                    onValueChange={(value) => setFormData(prev => ({ ...prev, account: value }))}
                    disabled={!isEditing}
                  >
                    <SelectTrigger className={!isEditing ? "bg-gray-50" : ""}>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {CASH_ACCOUNTS.map((account) => (
                        <SelectItem key={account} value={account}>
                          {account}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Right Column */}
              <div className="space-y-4">
                <div>
                  <Label htmlFor="recipient">Đối tượng nộp *</Label>
                  <Input
                    id="recipient"
                    value={formData.recipient}
                    onChange={(e) => setFormData(prev => ({ ...prev, recipient: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                    placeholder="Tên người/công ty nộp tiền"
                  />
                </div>

                <div>
                  <Label htmlFor="phone">Người nhận</Label>
                  <Input
                    id="phone"
                    value={formData.phone}
                    onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                    placeholder="Tên người nhận tiền"
                  />
                </div>

                <div>
                  <Label htmlFor="phone">Điện thoại</Label>
                  <Input
                    id="phone"
                    value={formData.phone}
                    onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                    placeholder="Số điện thoại"
                  />
                </div>

                <div>
                  <Label htmlFor="category">Loại thu *</Label>
                  <Select
                    value={formData.category}
                    onValueChange={(value) => setFormData(prev => ({ ...prev, category: value }))}
                    disabled={!isEditing}
                  >
                    <SelectTrigger className={!isEditing ? "bg-gray-50" : ""}>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {INCOME_CATEGORIES.map((category) => (
                        <SelectItem key={category} value={category}>
                          {category}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            {/* Description */}
            <div>
              <Label htmlFor="description">Diễn giải</Label>
              <Textarea
                id="description"
                value={formData.description}
                onChange={(e) => setFormData(prev => ({ ...prev, description: e.target.value }))}
                disabled={!isEditing}
                className={!isEditing ? "bg-gray-50" : ""}
                placeholder="Mô tả chi tiết về khoản thu..."
                rows={4}
              />
            </div>

            {/* Action Buttons */}
            <div className="flex justify-between pt-4 border-t">
              <div className="flex gap-2">
                {mode === "edit" && (
                  <>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => setShowDeleteDialog(true)}
                      disabled={deleteVoucherMutation.isPending}
                    >
                      <Trash2 className="w-4 h-4 mr-2" />
                      Xóa
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setIsEditing(true)}
                      disabled={isEditing}
                    >
                      <Edit className="w-4 h-4 mr-2" />
                      Sửa
                    </Button>
                  </>
                )}
              </div>

              <div className="flex gap-2">
                <Button
                  variant="outline"
                  onClick={onClose}
                  disabled={createVoucherMutation.isPending || updateVoucherMutation.isPending}
                >
                  <X className="w-4 h-4 mr-2" />
                  Đóng
                </Button>
                {isEditing && (
                  <Button
                    onClick={handleSave}
                    disabled={createVoucherMutation.isPending || updateVoucherMutation.isPending}
                    className="bg-green-600 hover:bg-green-700"
                  >
                    <Save className="w-4 h-4 mr-2" />
                    Lưu
                  </Button>
                )}
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Xác nhận xóa</AlertDialogTitle>
            <AlertDialogDescription>
              Bạn có muốn xóa phiếu thu <strong>{formData.voucherNumber}</strong> này không?
              <br />
              <span className="text-red-600">Hành động này không thể hoàn tác.</span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Bỏ qua</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              disabled={deleteVoucherMutation.isPending}
              className="bg-red-600 hover:bg-red-700"
            >
              Đồng ý
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
