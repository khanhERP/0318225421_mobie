import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation, useRoute } from "wouter";
import { useTranslation } from "@/lib/i18n";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { POSHeader } from "@/components/pos/header";
import { RightSidebar } from "@/components/ui/right-sidebar";
import { ArrowLeft, ClipboardCheck, Calendar, User, DollarSign, FileText, Package, ShoppingCart, Upload, X, CheckCircle } from "lucide-react";
import type { PurchaseOrder, PurchaseReceiptItem, Supplier } from "@shared/schema";

interface PurchaseViewPageProps {
  onLogout: () => void;
}

export default function PurchaseViewPage({ onLogout }: PurchaseViewPageProps) {
  const { t, currentLanguage } = useTranslation();
  const [, navigate] = useLocation();
  const [match, params] = useRoute("/purchases/view/:id");
  const purchaseId = params?.id ? parseInt(params.id) : null;
  const [isEditMode, setIsEditMode] = useState(false);
  
  // Form state for editing
  const [formData, setFormData] = useState({
    supplierId: "",
    purchaseDate: "",
    purchaseType: "",
    employeeId: "",
    notes: ""
  });

  // Fetch purchase receipt details
  const { data: purchaseOrder, isLoading: isOrderLoading, error: orderError } = useQuery<PurchaseOrder>({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts", purchaseId],
    queryFn: async () => {
      if (!purchaseId) throw new Error("Purchase ID not found");

      console.log('🔍 Fetching purchase receipt with ID:', purchaseId);
      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts/${purchaseId}`);

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Purchase receipt not found');
        }
        throw new Error(`Failed to fetch purchase receipt: ${response.status}`);
      }

      const data = await response.json();
      console.log('📦 Purchase receipt details:', data);
      return data;
    },
    enabled: !!purchaseId,
    retry: 1,
  });

  // Fetch purchase receipt items
  const { data: purchaseItems = [], isLoading: isItemsLoading } = useQuery<PurchaseReceiptItem[]>({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts", purchaseId, "items"],
    queryFn: async () => {
      if (!purchaseId) throw new Error("Purchase ID not found");

      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts/${purchaseId}/items`);
      if (!response.ok) {
        throw new Error('Failed to fetch purchase receipt items');
      }

      const data = await response.json();
      console.log('📦 Purchase receipt items:', data);
      return data;
    },
    enabled: !!purchaseId,
  });

  // Fetch suppliers for name lookup
  const { data: suppliers = [] } = useQuery<Supplier[]>({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/suppliers"],
  });

  // Initialize form data when purchase order loads
  useEffect(() => {
    if (purchaseOrder) {
      setFormData({
        supplierId: purchaseOrder.supplierId?.toString() || "",
        purchaseDate: purchaseOrder.purchaseDate || purchaseOrder.actualDeliveryDate || "",
        purchaseType: purchaseOrder.purchaseType || "",
        employeeId: purchaseOrder.employeeId?.toString() || "",
        notes: purchaseOrder.notes || ""
      });
    }
  }, [purchaseOrder]);

  // Fetch employees for display
  const { data: employees = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/employees"],
    select: (data: any[]) =>
      (data || []).map((emp: any) => ({
        id: emp.id,
        name: emp.name || `${emp.firstName || ''} ${emp.lastName || ''}`.trim() || 'Unnamed Employee',
      })),
  });

  if (!purchaseId) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-gray-900 dark:to-gray-800">
        <POSHeader />
        <RightSidebar />
        <div className="container mx-auto pt-16 px-6">
          <div className="text-center py-12">
            <h1 className="text-2xl font-bold text-red-600">Invalid Purchase Receipt ID</h1>
            <Button onClick={() => navigate('/purchases')} className="mt-4">
              Back to Purchases
            </Button>
          </div>
        </div>
      </div>
    );
  }

  const getSupplierName = (supplierId: number) => {
    const supplier = suppliers.find(s => s.id === supplierId);
    return supplier?.name || t('purchases.unknownSupplier');
  };

  const getEmployeeName = (employeeId: number) => {
    const employee = employees.find(e => e.id === employeeId);
    return employee?.name || 'Không xác định';
  };

  const formatCurrency = (amount: string) => {
    const locale = {
      ko: 'ko-KR',
      en: 'en-US',
      vi: 'vi-VN'
    }[currentLanguage] || 'en-US';

    const currency = {
      ko: 'KRW',
      en: 'USD',
      vi: 'VND'
    }[currentLanguage] || 'USD';

    return new Intl.NumberFormat(locale, {
      style: 'currency',
      currency: currency
    }).format(parseFloat(amount || '0'));
  };

  const formatDate = (dateString: string) => {
    if (!dateString) return '-';
    try {
      return new Date(dateString).toLocaleDateString({
        ko: 'ko-KR',
        en: 'en-US',
        vi: 'vi-VN'
      }[currentLanguage] || 'en-US');
    } catch (error) {
      console.error('Date parsing error:', error);
      return '-';
    }
  };

  if (isOrderLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-gray-900 dark:to-gray-800">
        <POSHeader />
        <RightSidebar />
        <div className="container mx-auto pt-16 px-6">
          <div className="text-center py-12">
            <ClipboardCheck className="w-16 h-16 text-gray-300 mx-auto mb-4 animate-pulse" />
            <p className="text-gray-500">{t("purchases.loadingOrders")}</p>
          </div>
        </div>
      </div>
    );
  }

  if (orderError) {
    console.error('❌ Error loading purchase receipt:', orderError);
    return (
      <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-gray-900 dark:to-gray-800">
        <POSHeader />
        <RightSidebar />
        <div className="container mx-auto pt-16 px-6">
          <div className="text-center py-12">
            <h1 className="text-2xl font-bold text-red-600">
              {orderError.message === 'Purchase receipt not found' 
                ? 'Purchase Receipt Not Found' 
                : 'Error Loading Purchase Receipt'}
            </h1>
            <p className="text-gray-600 mt-2">
              Purchase Receipt ID: {purchaseId}
            </p>
            <p className="text-sm text-gray-500 mt-1">
              {orderError.message}
            </p>
            <Button onClick={() => navigate('/purchases')} className="mt-4">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Purchases
            </Button>
          </div>
        </div>
      </div>
    );
  }

  if (!isOrderLoading && !purchaseOrder) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-gray-900 dark:to-gray-800">
        <POSHeader />
        <RightSidebar />
        <div className="container mx-auto pt-16 px-6">
          <div className="text-center py-12">
            <h1 className="text-2xl font-bold text-red-600">Purchase Receipt Not Found</h1>
            <p className="text-gray-600 mt-2">
              Purchase Receipt ID: {purchaseId}
            </p>
            <Button onClick={() => navigate('/purchases')} className="mt-4">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Purchases
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-gray-900 dark:to-gray-800">
      <POSHeader />
      <RightSidebar />

      <div className="container mx-auto px-4 py-6 max-w-7xl pt-16">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => navigate("/purchases")}
            >
              <ArrowLeft className="h-4 w-4 mr-2" />
              {t("common.back")}
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                {t("purchases.viewPurchaseOrder")}
              </h1>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                {t("purchases.viewOrderDescription")}
              </p>
            </div>
          </div>
          <Badge 
            className={`px-3 py-1 text-sm font-medium ${
              purchaseOrder.status === 'completed' ? 'bg-green-100 text-green-800' :
              purchaseOrder.status === 'pending' ? 'bg-yellow-100 text-yellow-800' :
              purchaseOrder.status === 'cancelled' ? 'bg-red-100 text-red-800' :
              'bg-gray-100 text-gray-800'
            }`}
          >
            {purchaseOrder.status || 'pending'}
          </Badge>
        </div>

        <div className="space-y-6">
          {/* Order Details Card */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <ShoppingCart className="h-5 w-5" />
                {t("purchases.orderDetails")}
              </CardTitle>
              <CardDescription>
                {t("purchases.orderDetailsDescription")}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4">
                      {/* Supplier */}
                      <div className="space-y-2">
                        <label className="text-xs sm:text-sm font-medium">
                          {t("purchases.supplier")} <span className="text-red-500">*</span>
                        </label>
                        <Select 
                          disabled={!isEditMode} 
                          value={formData.supplierId}
                          onValueChange={(value) => setFormData(prev => ({ ...prev, supplierId: value }))}
                        >
                          <SelectTrigger className="h-9 sm:h-10">
                            <SelectValue placeholder={getSupplierName(parseInt(formData.supplierId))} />
                          </SelectTrigger>
                          <SelectContent>
                            {suppliers.map((supplier: any) => (
                              <SelectItem key={supplier.id} value={supplier.id.toString()}>
                                {supplier.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      {/* Receipt Number */}
                      <div className="space-y-2">
                        <label className="text-xs sm:text-sm font-medium">
                          Số phiếu nhập <span className="text-red-500">*</span>
                        </label>
                        <Input
                          value={purchaseOrder.receiptNumber || purchaseOrder.poNumber || `PR-${purchaseOrder.id}`}
                          disabled
                          className="h-9 sm:h-10 text-sm bg-gray-100"
                        />
                      </div>

                      {/* Purchase Date */}
                      <div className="space-y-2">
                        <label className="text-xs sm:text-sm font-medium">
                          {t("purchases.purchaseDate")} <span className="text-red-500">*</span>
                        </label>
                        <Input
                          type="date"
                          value={formData.purchaseDate}
                          disabled={!isEditMode}
                          onChange={(e) => setFormData(prev => ({ ...prev, purchaseDate: e.target.value }))}
                          className="h-9 sm:h-10 text-sm"
                        />
                      </div>

                      {/* Purchase Type */}
                      <div className="space-y-2">
                        <label className="text-xs sm:text-sm font-medium">
                          {t("purchases.purchaseType")} <span className="text-red-500">*</span>
                        </label>
                        <Select 
                          disabled={!isEditMode} 
                          value={formData.purchaseType}
                          onValueChange={(value) => setFormData(prev => ({ ...prev, purchaseType: value }))}
                        >
                          <SelectTrigger className="h-9 sm:h-10">
                            <SelectValue placeholder={formData.purchaseType || "Không xác định"} />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="raw_materials">{t("purchases.rawMaterials")}</SelectItem>
                            <SelectItem value="expenses">{t("purchases.expenses")}</SelectItem>
                            <SelectItem value="others">{t("purchases.others")}</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      {/* Employee Assignment */}
                      <div className="space-y-2">
                        <label className="text-xs sm:text-sm font-medium">{t("purchases.assignedTo")}</label>
                        <Select 
                          disabled={!isEditMode} 
                          value={formData.employeeId}
                          onValueChange={(value) => setFormData(prev => ({ ...prev, employeeId: value }))}
                        >
                          <SelectTrigger className="h-9 sm:h-10">
                            <SelectValue placeholder={formData.employeeId ? getEmployeeName(parseInt(formData.employeeId)) : "Không xác định"} />
                          </SelectTrigger>
                          <SelectContent>
                            {employees.map((employee: any) => (
                              <SelectItem key={employee.id} value={employee.id.toString()}>
                                {employee.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      {/* Notes */}
                      <div className="space-y-2 sm:col-span-2">
                        <label className="text-xs sm:text-sm font-medium">Ghi chú</label>
                        <Textarea
                          value={formData.notes}
                          disabled={!isEditMode}
                          onChange={(e) => setFormData(prev => ({ ...prev, notes: e.target.value }))}
                          placeholder="Nhập ghi chú cho phiếu nhập..."
                          className="h-20 text-sm resize-none"
                        />
                      </div>

                      {/* File Attachments Display */}
                      <div className="space-y-2 sm:col-span-2">
                        <label className="text-xs sm:text-sm font-medium flex items-center gap-2">
                          <Upload className="h-3 w-3 sm:h-4 sm:w-4" />
                          Đính kèm tệp
                        </label>
                        <div className="border border-dashed border-gray-300 rounded-md p-3 sm:p-4 text-center bg-gray-50/50 min-h-[36px] sm:min-h-[42px] flex items-center justify-center">
                          <div className="flex items-center gap-2">
                            <Upload className="h-3 w-3 sm:h-4 sm:w-4 text-gray-400" />
                            <span className="text-xs sm:text-sm text-gray-600">
                              Không có tệp đính kèm
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
            </CardContent>
          </Card>

          {/* Items Card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <Package className="h-5 w-5" />
                    {t("purchases.items")} ({purchaseItems.length})
                  </CardTitle>
                  <CardDescription>
                    {t("purchases.itemsDescription")}
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                  <div className="overflow-x-auto border rounded-lg">
                    <Table className="min-w-[800px]">
                      <TableHeader>
                        <TableRow className="bg-gray-50">
                          <TableHead className="w-8 sm:w-12 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            No
                          </TableHead>
                          <TableHead className="min-w-[120px] sm:min-w-[180px] p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Sản phẩm
                          </TableHead>
                          <TableHead className="w-12 sm:w-20 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Đơn vị
                          </TableHead>
                          <TableHead className="w-16 sm:w-24 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            SL
                          </TableHead>
                          <TableHead className="w-20 sm:w-28 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Đơn giá
                          </TableHead>
                          <TableHead className="w-20 sm:w-28 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Thành tiền
                          </TableHead>
                          <TableHead className="w-12 sm:w-20 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            %CK
                          </TableHead>
                          <TableHead className="w-20 sm:w-28 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Chiết khấu
                          </TableHead>
                          <TableHead className="w-24 sm:w-32 text-center p-1 sm:p-2 font-bold text-xs sm:text-sm">
                            Tổng tiền
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                    <TableBody>
                      {purchaseItems.length === 0 ? (
                        <TableRow>
                          <TableCell
                            colSpan={9}
                            className="text-center py-12 text-gray-500 dark:text-gray-400"
                          >
                            <div className="flex flex-col items-center">
                              <Package className="h-12 w-12 mb-3 opacity-50" />
                              <p className="text-lg font-medium mb-1">
                                {t("purchases.noItemsSelected")}
                              </p>
                            </div>
                          </TableCell>
                        </TableRow>
                      ) : (
                        purchaseItems.map((item, index) => {
                          const subtotal = item.quantity * parseFloat(item.unitPrice || '0');
                          const discountPercent = 0; // No discount data available
                          const discountAmount = subtotal * (discountPercent / 100);
                          const finalTotal = subtotal - discountAmount;

                          return (
                            <TableRow
                              key={index}
                              className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors"
                            >
                              {/* No */}
                              <TableCell className="text-center font-semibold text-gray-600 p-1 sm:p-2">
                                <div className="flex items-center justify-center w-8 h-8 bg-blue-100 text-blue-600 rounded-full text-xs font-bold">
                                  {index + 1}
                                </div>
                              </TableCell>

                              {/* Product */}
                              <TableCell className="p-1 sm:p-2">
                                <div className="flex flex-col">
                                  <p className="font-medium text-gray-900 dark:text-white line-clamp-2 leading-tight text-xs sm:text-sm">
                                    {item.productName || "Unknown Product"}
                                  </p>
                                  {item.sku && (
                                    <p className="text-xs text-gray-500 bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded mt-1 inline-block w-fit">
                                      SKU: {item.sku}
                                    </p>
                                  )}
                                </div>
                              </TableCell>

                              {/* Unit */}
                              <TableCell className="text-center p-1 sm:p-2">
                                <span className="text-xs sm:text-sm text-gray-600 bg-gray-50 px-1.5 py-0.5 rounded-full">
                                  Cái
                                </span>
                              </TableCell>

                              {/* Quantity */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="number"
                                  value={item.quantity}
                                  disabled={!isEditMode}
                                  className="w-16 text-center text-sm h-8"
                                />
                              </TableCell>

                              {/* Unit Price */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="text"
                                  value={parseFloat(item.unitPrice || '0').toLocaleString('ko-KR')}
                                  disabled={!isEditMode}
                                  className="w-20 text-right text-sm h-8"
                                />
                              </TableCell>

                              {/* Subtotal */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="text"
                                  value={subtotal.toLocaleString('ko-KR')}
                                  disabled
                                  className="w-20 text-right font-medium text-sm h-8"
                                />
                              </TableCell>

                              {/* Discount Percent */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="number"
                                  value={discountPercent}
                                  disabled={!isEditMode}
                                  className="w-12 text-center text-sm h-8"
                                />
                              </TableCell>

                              {/* Discount Amount */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="text"
                                  value={discountAmount.toLocaleString('ko-KR')}
                                  disabled
                                  className="w-20 text-right font-medium text-sm h-8 border-red-300"
                                />
                              </TableCell>

                              {/* Total */}
                              <TableCell className="p-1 sm:p-2">
                                <Input
                                  type="text"
                                  value={finalTotal.toLocaleString('ko-KR')}
                                  disabled
                                  className="w-24 text-right font-bold text-green-600 bg-green-50 border-green-300 text-sm h-8"
                                />
                              </TableCell>
                            </TableRow>
                          );
                        })
                      )}

                      {/* Summary Row */}
                      {purchaseItems.length > 0 && (
                        <TableRow className="bg-blue-50 dark:bg-blue-900/20 border-t-2 border-blue-200 font-semibold">
                          {/* No */}
                          <TableCell className="text-center p-1 sm:p-2">
                            <div className="flex items-center justify-center w-8 h-8 bg-blue-200 text-blue-800 rounded-full text-xs font-bold">
                              Σ
                            </div>
                          </TableCell>

                          {/* Total Label */}
                          <TableCell className="p-1 sm:p-2 font-bold text-blue-800 text-xs sm:text-sm">
                            TỔNG CỘNG
                          </TableCell>

                          {/* Unit */}
                          <TableCell className="text-center p-1 sm:p-2">
                            <span className="text-xs sm:text-sm text-blue-600">-</span>
                          </TableCell>

                          {/* Total Quantity */}
                          <TableCell className="p-1 sm:p-2">
                            <div className="w-16 text-center font-bold text-blue-800 bg-blue-100 border border-blue-300 rounded px-1.5 py-0.5 text-xs sm:text-sm">
                              {purchaseItems.reduce((sum, item) => sum + item.quantity, 0)}
                            </div>
                          </TableCell>

                          {/* Unit Price - not displayed */}
                          <TableCell className="p-1 sm:p-2">
                            <span className="text-xs sm:text-sm text-blue-600">-</span>
                          </TableCell>

                          {/* Total Subtotal */}
                          <TableCell className="p-1 sm:p-2">
                            <div className="w-20 text-right font-bold text-blue-800 bg-blue-100 border border-blue-300 rounded px-1.5 py-0.5 text-xs sm:text-sm">
                              {purchaseItems.reduce((sum, item) => {
                                const subtotal = item.quantity * parseFloat(item.unitPrice || '0');
                                return sum + subtotal;
                              }, 0).toLocaleString("ko-KR")}
                            </div>
                          </TableCell>

                          {/* Discount Percent - not displayed */}
                          <TableCell className="p-1 sm:p-2">
                            <span className="text-xs sm:text-sm text-blue-600">-</span>
                          </TableCell>

                          {/* Total Discount */}
                          <TableCell className="p-1 sm:p-2">
                            <div className="w-20 text-right font-bold text-red-800 bg-red-100 border border-red-300 rounded px-1.5 py-0.5 text-xs sm:text-sm">
                              0
                            </div>
                          </TableCell>

                          {/* Final Total */}
                          <TableCell className="p-1 sm:p-2">
                            <div className="w-24 text-right font-bold text-green-800 bg-green-100 border border-green-300 rounded px-1.5 py-0.5 text-xs sm:text-sm">
                              {formatCurrency(purchaseOrder.total || '0')}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Action Buttons */}
          <div className="flex gap-4 justify-end">
            <Button
              variant="outline"
              onClick={() => navigate('/purchases')}
            >
              <ArrowLeft className="h-4 w-4 mr-2" />
              {t("common.back")}
            </Button>
            {!isEditMode ? (
              <Button
                onClick={() => setIsEditMode(true)}
                className="bg-blue-600 hover:bg-blue-700"
              >
                <FileText className="h-4 w-4 mr-2" />
                Chỉnh sửa
              </Button>
            ) : (
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  onClick={() => {
                    setIsEditMode(false);
                    // Reset form data to original values
                    if (purchaseOrder) {
                      setFormData({
                        supplierId: purchaseOrder.supplierId?.toString() || "",
                        purchaseDate: purchaseOrder.purchaseDate || purchaseOrder.actualDeliveryDate || "",
                        purchaseType: purchaseOrder.purchaseType || "",
                        employeeId: purchaseOrder.employeeId?.toString() || "",
                        notes: purchaseOrder.notes || ""
                      });
                    }
                  }}
                >
                  Hủy
                </Button>
                <Button
                  className="bg-green-600 hover:bg-green-700"
                  onClick={async () => {
                    try {
                      // Validation for required fields
                      const missingFields = [];
                      
                      if (!formData.supplierId || formData.supplierId === '0') {
                        missingFields.push('Nhà cung cấp');
                      }
                      
                      if (!purchaseOrder.receiptNumber && !purchaseOrder.poNumber) {
                        missingFields.push('Số phiếu nhập');
                      }
                      
                      if (!formData.purchaseDate) {
                        missingFields.push('Ngày nhập');
                      }
                      
                      if (!formData.purchaseType) {
                        missingFields.push('Loại mua hàng');
                      }

                      if (missingFields.length > 0) {
                        alert(`Vui lòng nhập đầy đủ thông tin cho các trường bắt buộc sau:\n- ${missingFields.join('\n- ')}`);
                        return;
                      }

                      const response = await fetch(`https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts/${purchaseId}`, {
                        method: 'PUT',
                        headers: {
                          'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                          supplierId: parseInt(formData.supplierId) || null,
                          purchaseDate: formData.purchaseDate || null,
                          purchaseType: formData.purchaseType || null,
                          employeeId: parseInt(formData.employeeId) || null,
                          notes: formData.notes || null
                        })
                      });

                      if (response.ok) {
                        setIsEditMode(false);
                        // Refresh the data
                        window.location.reload();
                      } else {
                        console.error('Failed to update purchase receipt');
                      }
                    } catch (error) {
                      console.error('Error updating purchase receipt:', error);
                    }
                  }}
                >
                  <CheckCircle className="h-4 w-4 mr-2" />
                  Lưu
                </Button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}