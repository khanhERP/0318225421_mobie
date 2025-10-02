
import React, { useState, useEffect } from "react";
import { useMutation, useQueryClient, useQuery } from "@tanstack/react-query";
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

interface ExpenseVoucher {
  id?: string;
  voucherNumber: string;
  date: string;
  amount: number;
  account: string;
  recipient: string;
  receiverName?: string;
  phone: string;
  category: string;
  description: string;
}

interface ExpenseVoucherModalProps {
  isOpen: boolean;
  onClose: () => void;
  voucher?: ExpenseVoucher | null;
  mode: "create" | "edit";
}

const EXPENSE_CATEGORIES = [
  "Tiếp khách",
  "Văn phòng phẩm", 
  "Điện nước",
  "Thuê mặt bằng",
  "Lương nhân viên",
  "Bảo hiểm",
  "Sửa chữa",
  "Thanh toán Nhà cung cấp",
  "Trả lại tiền cho khách hàng",
  "Chi khác",
];

const CASH_ACCOUNTS = [
  "Tiền mặt",
  "Ngân hàng",
  "Ví điện tử",
];

export default function ExpenseVoucherModal({ 
  isOpen, 
  onClose, 
  voucher, 
  mode 
}: ExpenseVoucherModalProps) {
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [isEditing, setIsEditing] = useState(mode === "create");
  const queryClient = useQueryClient();

  // Fetch customers
  const { data: customers = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/customers"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/customers");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching customers:', error);
        return [];
      }
    },
  });

  // Fetch employees
  const { data: employees = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/employees"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/employees");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching employees:', error);
        return [];
      }
    },
  });

  // Fetch suppliers
  const { data: suppliers = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/suppliers"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/suppliers");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching suppliers:', error);
        return [];
      }
    },
  });
  
  const [formData, setFormData] = useState<ExpenseVoucher>({
    voucherNumber: "",
    date: new Date().toISOString().split('T')[0],
    amount: 0,
    account: "Tiền mặt",
    recipient: "",
    receiverName: "",
    phone: "",
    category: "Chi khác",
    description: "",
  });

  useEffect(() => {
    if (voucher && mode === "edit") {
      setFormData(voucher);
      setIsEditing(false);
    } else if (mode === "create") {
      // Set empty voucher number for new voucher - will be generated on save if empty
      setFormData(prev => ({
        ...prev,
        voucherNumber: "",
      }));
      setIsEditing(true);
    }
  }, [voucher, mode]);

  const createVoucherMutation = useMutation({
    mutationFn: async (data: ExpenseVoucher) => {
      console.log("Creating expense voucher with data:", data);
      const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers", {
        method: "POST",
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
      console.log("Expense voucher created successfully:", data);
      toast({
        title: "Thành công",
        description: `Đã tạo phiếu chi ${formData.voucherNumber} thành công`,
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      onClose();
    },
    onError: (error) => {
      console.error("Failed to create expense voucher:", error);
      const errorMessage = error instanceof Error ? error.message : "Không thể tạo phiếu chi";
      toast({
        title: "Lỗi tạo phiếu chi",
        description: errorMessage,
        variant: "destructive",
      });
    },
  });

  const updateVoucherMutation = useMutation({
    mutationFn: async (data: ExpenseVoucher) => {
      console.log("Updating expense voucher with data:", data);
      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers/${data.id}`, {
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
      console.log("Expense voucher updated successfully:", data);
      toast({
        title: "Thành công", 
        description: `Đã cập nhật phiếu chi ${formData.voucherNumber} thành công`,
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      setIsEditing(false);
    },
    onError: (error) => {
      console.error("Failed to update expense voucher:", error);
      const errorMessage = error instanceof Error ? error.message : "Không thể cập nhật phiếu chi";
      toast({
        title: "Lỗi cập nhật phiếu chi",
        description: errorMessage,
        variant: "destructive",
      });
    },
  });

  const deleteVoucherMutation = useMutation({
    mutationFn: async (id: string) => {
      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers/${id}`, {
        method: "DELETE",
      });
      if (!response.ok) throw new Error("Failed to delete expense voucher");
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Thành công",
        description: "Đã xóa phiếu chi",
      });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers"] });
      queryClient.invalidateQueries({ queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"] });
      setShowDeleteDialog(false);
      onClose();
    },
    onError: (error) => {
      toast({
        title: "Lỗi", 
        description: "Không thể xóa phiếu chi",
        variant: "destructive",
      });
    },
  });

  const handleSave = async () => {
    // Auto-generate voucher number if empty
    let voucherNumber = formData.voucherNumber?.trim();
    if (!voucherNumber) {
      try {
        // Get the next sequence number from server
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers/next-sequence");
        if (!response.ok) throw new Error("Failed to get next sequence");
        
        const { nextSequence } = await response.json();
        const currentYear = new Date().getFullYear();
        const yearSuffix = currentYear.toString().slice(-2); // Last 2 digits of year
        const sequenceStr = nextSequence.toString().padStart(6, '0'); // Format as 6-digit string
        voucherNumber = `PC${sequenceStr}/${yearSuffix}`;
        
        // Update form data with generated voucher number
        setFormData(prev => ({ ...prev, voucherNumber }));
      } catch (error) {
        toast({
          title: "Lỗi",
          description: "Không thể tạo số phiếu chi tự động",
          variant: "destructive",
        });
        return;
      }
    }

    if (!formData.recipient?.trim()) {
      toast({
        title: "Lỗi", 
        description: "Vui lòng nhập đối tượng nhận",
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
        description: "Vui lòng chọn ngày chi",
        variant: "destructive",
      });
      return;
    }

    // Prepare clean data for submission
    const cleanData = {
      ...formData,
      voucherNumber: voucherNumber,
      recipient: formData.recipient.trim(),
      account: formData.account || "Tiền mặt",
      category: formData.category || "Chi khác",
      date: formData.date.trim(),
      receiverName: formData.receiverName?.trim() || "",
      phone: formData.phone?.trim() || "",
      description: formData.description?.trim() || "",
      amount: Number(formData.amount),
    };

    console.log("Saving expense voucher:", cleanData);

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

  // Get recipient options based on selected category
  const getRecipientOptions = () => {
    switch (formData.category) {
      case "Trả lại tiền cho khách hàng":
        return customers.map(customer => ({
          id: customer.id,
          name: customer.name,
          phone: customer.phone || "",
        }));
      case "Lương nhân viên":
        return employees.map(employee => ({
          id: employee.id,
          name: employee.name,
          phone: employee.phone || "",
        }));
      default:
        return suppliers.map(supplier => ({
          id: supplier.id,
          name: supplier.name,
          phone: supplier.phone || "",
        }));
    }
  };

  const recipientOptions = getRecipientOptions();

  // Handle category change - reset recipient when category changes
  const handleCategoryChange = (newCategory: string) => {
    setFormData(prev => ({
      ...prev,
      category: newCategory,
      recipient: "", // Reset recipient when category changes
      receiverName: "",
      phone: "",
    }));
  };

  // Handle recipient selection
  const handleRecipientChange = (recipientId: string) => {
    const selectedRecipient = recipientOptions.find(option => option.id.toString() === recipientId);
    if (selectedRecipient) {
      setFormData(prev => ({
        ...prev,
        recipient: selectedRecipient.name,
        receiverName: selectedRecipient.name,
        phone: selectedRecipient.phone,
      }));
    }
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
              <span>Phiếu chi</span>
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-6 p-4">
            {/* Form Fields */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Left Column */}
              <div className="space-y-4">
                <div>
                  <Label htmlFor="voucherNumber">Số phiếu chi *</Label>
                  <Input
                    id="voucherNumber"
                    value={formData.voucherNumber}
                    onChange={(e) => setFormData(prev => ({ ...prev, voucherNumber: e.target.value }))}
                    disabled={!isEditing}
                    className={!isEditing ? "bg-gray-50" : ""}
                    placeholder="Để trống để hệ thống tự sinh (PCxxxxxx/YY)"
                  />
                </div>

                <div>
                  <Label htmlFor="date">Ngày chi *</Label>
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
                </div>

                <div>
                  <Label htmlFor="account">Tài khoản chi *</Label>
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
                  <Label htmlFor="recipient">
                    Đối tượng nhận * 
                    <span className="text-sm text-gray-500 ml-2">
                      ({formData.category === "Trả lại tiền cho khách hàng" ? "Khách hàng" : 
                        formData.category === "Lương nhân viên" ? "Nhân viên" : "Nhà cung cấp"})
                    </span>
                  </Label>
                  {isEditing ? (
                    <Select
                      value={formData.recipient}
                      onValueChange={handleRecipientChange}
                      disabled={!isEditing}
                    >
                      <SelectTrigger className={!isEditing ? "bg-gray-50" : ""}>
                        <SelectValue placeholder={`Chọn ${formData.category === "Trả lại tiền cho khách hàng" ? "khách hàng" : 
                          formData.category === "Lương nhân viên" ? "nhân viên" : "nhà cung cấp"}`} />
                      </SelectTrigger>
                      <SelectContent>
                        {recipientOptions.map((option) => (
                          <SelectItem key={option.id} value={option.id.toString()}>
                            {option.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input
                      id="recipient"
                      value={formData.recipient}
                      disabled={true}
                      className="bg-gray-50"
                      placeholder="Tên người/công ty nhận tiền"
                    />
                  )}
                </div>

                <div>
                  <Label htmlFor="receiverName">Người nhận</Label>
                  <Input
                    id="receiverName"
                    value={formData.receiverName || ""}
                    onChange={(e) => setFormData(prev => ({ ...prev, receiverName: e.target.value }))}
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
                  <Label htmlFor="category">Loại chi *</Label>
                  <Select
                    value={formData.category}
                    onValueChange={handleCategoryChange}
                    disabled={!isEditing}
                  >
                    <SelectTrigger className={!isEditing ? "bg-gray-50" : ""}>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {EXPENSE_CATEGORIES.map((category) => (
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
                placeholder="Mô tả chi tiết về khoản chi..."
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
              Bạn có muốn xóa phiếu chi <strong>{formData.voucherNumber}</strong> này không?
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
