import {
  categories,
  products,
  transactions,
  transactions as transactionsTable,
  transactionItems,
  transactionItems as transactionItemsTable,
  employees,
  attendanceRecords,
  tables,
  orders,
  orderItems as orderItemsTable,
  storeSettings,
  suppliers,
  customers,
  pointTransactions,
  invoiceTemplates,
  eInvoiceConnections,
  inventoryTransactions,
  invoices,
  invoiceItems,
  printerConfigs, // Import printerConfigs schema
  purchaseReceipts, // Maps to purchase_receipts table
  purchaseReceiptItems, // Maps to purchase_receipt_items table
  purchaseReceiptDocuments, // Maps to purchase_receipt_documents table
  incomeVouchers, // Import incomeVouchers schema
  expenseVouchers, // Import expenseVouchers schema
  type PurchaseReceipt,
  type PurchaseReceiptItem,
  type PurchaseReceiptDocument,
  type InsertPurchaseReceipt,
  type InsertPurchaseReceiptItem,
  type InsertPurchaseReceiptDocument,
} from "@shared/schema";
import { db, dbManager, getTenantDatabase as getDbForTenant } from "./db";
import {
  eq,
  ilike,
  and,
  gte,
  lte,
  or,
  sql,
  desc,
  not,
  like,
  ne,
} from "drizzle-orm";

// Validate database connection on module load
if (!db) {
  console.error("❌ CRITICAL: Database connection is undefined on module load");
  throw new Error("Database connection failed to initialize");
}

// Additional validation to ensure db has required methods
if (!db.select || typeof db.select !== "function") {
  console.error("❌ CRITICAL: Database connection is missing select method");
  throw new Error("Database connection is invalid - missing required methods");
}

console.log("✅ Database connection validated successfully");

export interface IStorage {
  // Categories
  getCategories(): Promise<Category[]>;
  createCategory(category: InsertCategory): Promise<Category>;
  updateCategory(
    id: number,
    updateData: Partial<InsertCategory>,
  ): Promise<Category>;
  deleteCategory(id: number): Promise<void>;

  // Products
  getProducts(): Promise<Product[]>;
  getProductsByCategory(
    categoryId: number,
    includeInactive?: boolean,
  ): Promise<Product[]>;
  getProduct(id: number): Promise<Product | undefined>;
  getProductBySku(sku: string): Promise<Product | undefined>;
  searchProducts(query: string, includeInactive?: boolean): Promise<Product[]>;
  createProduct(product: InsertProduct): Promise<Product>;
  updateProduct(
    id: number,
    product: Partial<InsertProduct>,
  ): Promise<Product | undefined>;
  deleteProduct(id: number): Promise<boolean>;
  deleteInactiveProducts(): Promise<number>;
  updateProductStock(
    id: number,
    quantity: number,
  ): Promise<Product | undefined>;

  // Inventory Management
  updateInventoryStock(
    productId: number,
    quantity: number,
    type: "add" | "subtract" | "set",
    notes?: string,
  ): Promise<Product | undefined>;

  // Transactions
  createTransaction(
    transaction: InsertTransaction,
    items: InsertTransactionItem[],
  ): Promise<Receipt>;
  getTransaction(id: number): Promise<Receipt | undefined>;
  getTransactionByTransactionId(
    transactionId: string,
  ): Promise<Receipt | undefined>;
  getTransactions(): Promise<Transaction[]>;

  // Employees
  getEmployees(): Promise<Employee[]>;
  getEmployee(id: number): Promise<Employee | undefined>;
  getEmployeeByEmployeeId(employeeId: string): Promise<Employee | undefined>;
  createEmployee(employee: InsertEmployee): Promise<Employee>;
  updateEmployee(
    id: number,
    employee: Partial<InsertEmployee>,
  ): Promise<Employee | undefined>;
  deleteEmployee(id: number): Promise<boolean>;
  getNextEmployeeId(): Promise<string>;

  // Attendance
  getAttendanceRecords(
    employeeId?: number,
    date?: string,
  ): Promise<AttendanceRecord[]>;
  getAttendanceRecord(id: number): Promise<AttendanceRecord | undefined>;
  getTodayAttendance(employeeId: number): Promise<AttendanceRecord | undefined>;
  clockIn(employeeId: number, notes?: string): Promise<AttendanceRecord>;
  clockOut(attendanceId: number): Promise<AttendanceRecord | undefined>;
  startBreak(attendanceId: number): Promise<AttendanceRecord | undefined>;
  endBreak(attendanceId: number): Promise<AttendanceRecord | undefined>;
  updateAttendanceStatus(
    id: number,
    status: string,
  ): Promise<AttendanceRecord | undefined>;
  getAttendanceRecordsByRange(
    startDate: string,
    endDate: string,
  ): Promise<AttendanceRecord[]>;

  // Tables
  getTables(): Promise<Table[]>;
  getTable(id: number): Promise<Table | undefined>;
  getTableByNumber(tableNumber: string): Promise<Table | undefined>;
  createTable(table: InsertTable): Promise<Table>;
  updateTable(
    id: number,
    table: Partial<InsertTable>,
  ): Promise<Table | undefined>;
  updateTableStatus(id: number, status: string): Promise<Table | undefined>;
  deleteTable(id: number): Promise<boolean>;

  // Orders
  getOrders(
    tableId?: number,
    status?: string,
    salesChannel?: string,
  ): Promise<Order[]>;
  getOrder(id: number): Promise<Order | undefined>;
  getOrderByNumber(orderNumber: string): Promise<Order | undefined>;
  createOrder(order: InsertOrder, items: InsertOrderItem[]): Promise<Order>;
  updateOrder(
    id: number,
    order: Partial<InsertOrder>,
  ): Promise<Order | undefined>;
  updateOrderStatus(
    id: number | string,
    status: string,
  ): Promise<Order | undefined>;
  addOrderItems(
    orderId: number,
    items: InsertOrderItem[],
  ): Promise<OrderItem[]>;
  removeOrderItem(itemId: number): Promise<boolean>;
  getOrderItems(orderId: number): Promise<OrderItem[]>;

  // Store Settings
  getStoreSettings(): Promise<StoreSettings>;
  updateStoreSettings(
    settings: Partial<InsertStoreSettings>,
  ): Promise<StoreSettings>;

  // Suppliers
  getSuppliers(): Promise<any>;
  getSupplier(id: number): Promise<any>;
  getSuppliersByStatus(status: string): Promise<any>;
  searchSuppliers(query: string): Promise<any>;
  createSupplier(data: InsertSupplier): Promise<any>;
  updateSupplier(id: number, data: Partial<InsertSupplier>): Promise<any>;
  deleteSupplier(id: number): Promise<boolean>;

  // Customers
  getCustomers(): Promise<Customer[]>;
  searchCustomers(query: string): Promise<Customer[]>;
  getCustomer(id: number): Promise<Customer | undefined>;
  getCustomerByCustomerId(customerId: string): Promise<Customer | undefined>;
  createCustomer(customer: InsertCustomer): Promise<Customer>;
  updateCustomer(
    id: number,
    customer: Partial<InsertCustomer>,
  ): Promise<Customer | undefined>;
  deleteCustomer(id: number): Promise<boolean>;
  updateCustomerVisit(
    id: number,
    amount: number,
    points: number,
  ): Promise<Customer | undefined>;

  // Point Management
  getCustomerPoints(
    customerId: number,
  ): Promise<{ points: number } | undefined>;
  updateCustomerPoints(
    customerId: number,
    points: number,
    description: string,
    type: "earned" | "redeemed" | "adjusted",
    employeeId?: number,
    orderId?: number,
  ): Promise<PointTransaction>;
  getPointHistory(
    customerId: number,
    limit?: number,
  ): Promise<PointTransaction[]>;

  getAllPointTransactions(limit?: number): Promise<PointTransaction[]>;

  getMembershipThresholds(): Promise<{ GOLD: number; VIP: number }>;
  updateMembershipThresholds(thresholds: {
    GOLD: number;
    VIP: number;
  }): Promise<{ GOLD: number; VIP: number }>;
  recalculateAllMembershipLevels(
    goldThreshold: number,
    vipThreshold: number,
  ): Promise<void>;

  getAllProducts(includeInactive?: boolean): Promise<Product[]>;
  getActiveProducts(): Promise<Product[]>;

  // Invoice methods
  getInvoices(): Promise<any[]>;
  getInvoice(id: number): Promise<any>;
  createInvoice(invoiceData: any): Promise<any>;
  updateInvoice(id: number, updateData: any): Promise<any>;
  deleteInvoice(id: number): Promise<boolean>;

  // Invoice template methods
  getInvoiceTemplates(): Promise<any[]>;
  getActiveInvoiceTemplates(): Promise<any[]>;
  createInvoiceTemplate(templateData: any): Promise<any>;
  updateInvoiceTemplate(id: number, templateData: any): Promise<any>;
  deleteInvoiceTemplate(id: number): Promise<boolean>;

  // E-invoice connections
  getEInvoiceConnections(): Promise<any[]>;
  getEInvoiceConnection(id: number): Promise<any>;
  createEInvoiceConnection(data: any): Promise<any>;
  updateEInvoiceConnection(id: number, data: any): Promise<any>;
  deleteEInvoiceConnection(id: number): Promise<boolean>;

  getEmployeeByEmail(email: string): Promise<Employee | undefined>;

  // Printer configuration management
  getPrinterConfigs(): Promise<PrinterConfig[]>;
  createPrinterConfig(configData: any): Promise<PrinterConfig>;
  updatePrinterConfig(
    id: number,
    configData: any,
  ): Promise<PrinterConfig | null>;
  deletePrinterConfig(id: number): Promise<boolean>;

  // Purchase Order Management
  getPurchaseOrders(): Promise<PurchaseReceipt[]>;
  getPurchaseOrder(id: number): Promise<PurchaseReceipt | null>;
  getPurchaseOrdersBySupplier(supplierId: number): Promise<PurchaseReceipt[]>;
  getPurchaseOrdersByStatus(status: string): Promise<PurchaseReceipt[]>;
  searchPurchaseOrders(query: string): Promise<PurchaseReceipt[]>;
  createPurchaseOrder(
    orderData: InsertPurchaseReceipt,
    items: InsertPurchaseReceiptItem[],
  ): Promise<PurchaseReceipt>;
  updatePurchaseOrder(
    id: number,
    updateData: Partial<InsertPurchaseReceipt>,
  ): Promise<PurchaseReceipt | null>;
  deletePurchaseOrder(id: number): Promise<boolean>;
  updatePurchaseOrderStatus(
    id: number,
    status: string,
  ): Promise<PurchaseReceipt | null>;
  getNextPONumber(): Promise<string>;
  checkReceiptNumberExists(receiptNumber: string, excludeId?: number): Promise<boolean>;
  getPurchaseOrdersWithDetails(options?: any): Promise<any>; // Added method

  // Purchase Receipt Management (new method names)
  getPurchaseReceipts(options: any): Promise<PurchaseReceipt[]>;
  createPurchaseReceipt(
    receiptData: InsertPurchaseReceipt,
    items: InsertPurchaseReceiptItem[],
  ): Promise<PurchaseReceipt>;
  uploadPurchaseReceiptDocument(
    documentData: InsertPurchaseReceiptDocument,
  ): Promise<PurchaseReceiptDocument>;

  // Purchase Order Items Management (keep for compatibility)
  getPurchaseOrderItems(
    purchaseOrderId: number,
  ): Promise<PurchaseReceiptItem[]>;
  addPurchaseOrderItems(
    purchaseOrderId: number,
    items: InsertPurchaseReceiptItem[],
  ): Promise<PurchaseReceiptItem[]>;
  updatePurchaseOrderItem(
    id: number,
    updateData: Partial<InsertPurchaseReceiptItem>,
  ): Promise<PurchaseReceiptItem | null>;
  deletePurchaseOrderItem(id: number): Promise<boolean>;
  receiveItems(
    purchaseOrderId: number,
    receivedItems: Array<{
      id: number;
      receivedQuantity: number;
      productId?: number;
    }>,
  ): Promise<{ success: boolean; status: string }>;

  // Purchase Order Documents Management
  getPurchaseOrderDocuments(
    purchaseOrderId: number,
  ): Promise<PurchaseReceiptDocument[]>;
  uploadPurchaseOrderDocument(
    documentData: InsertPurchaseReceiptDocument,
  ): Promise<PurchaseReceiptDocument>;
  deletePurchaseOrderDocument(id: number): Promise<boolean>;

  // Income Voucher methods
  getIncomeVouchers(): Promise<any[]>;
  createIncomeVoucher(voucherData: any): Promise<any>;
  updateIncomeVoucher(id: string, voucherData: any): Promise<any>;
  deleteIncomeVoucher(id: string): Promise<void>;

  // Expense Voucher methods
  getExpenseVouchers(): Promise<any[]>;
  createExpenseVoucher(voucherData: any): Promise<any>;
  updateExpenseVoucher(id: string, voucherData: any): Promise<any>;
  deleteExpenseVoucher(id: string): Promise<void>;
  getNextExpenseVoucherSequence(): Promise<number>;
}

// Define interfaces for the schemas
interface Category {
  id: number;
  name: string;
  isActive: boolean;
}

interface InsertCategory {
  name: string;
  isActive?: boolean;
}

interface Product {
  id: number;
  name: string;
  sku: string;
  price: number;
  stock: number;
  categoryId: number;
  productType: number;
  trackInventory: boolean;
  imageUrl: string | null;
  isActive: boolean;
  afterTaxPrice: number | null;
  priceIncludesTax: boolean; // Added for clarity
  beforeTaxPrice: number | null; // Added field
  floor: string; // Added floor field
}

interface InsertProduct {
  name: string;
  sku: string;
  price: number;
  stock: number;
  categoryId: number;
  productType?: number;
  trackInventory?: boolean;
  imageUrl?: string | null;
  isActive?: boolean;
  priceIncludesTax?: boolean; // Added for clarity
  beforeTaxPrice?: number | null; // Added field
  floor?: string; // Added floor field
  taxRate?: string; // Added for explicit tax rate input
}

interface Transaction {
  id: number;
  transactionId: string;
  customerId: number | null;
  employeeId: number | null;
  tableId: number | null;
  status: string;
  subtotal: number;
  tax: number;
  discount: number;
  total: number;
  amountReceived: number | null;
  change: number | null;
  paymentMethod: string;
  paymentStatus: string;
  createdAt: string;
  updatedAt: string;
}

interface InsertTransaction {
  transactionId: string;
  customerId?: number | null;
  employeeId?: number | null;
  tableId?: number | null;
  status: string;
  subtotal: number;
  tax: number;
  discount: number;
  total: number;
  amountReceived?: number | null;
  change?: number | null;
  paymentMethod: string;
  paymentStatus: string;
  createdAt?: Date;
}

interface TransactionItem {
  id: number;
  transactionId: number;
  productId: number;
  quantity: number;
  unitPrice: number;
  total: number;
  productName: string;
}

interface InsertTransactionItem {
  productId: number;
  quantity: number;
  unitPrice: number;
  total: number;
  productName: string;
}

interface Receipt extends Transaction {
  items: TransactionItem[];
}

interface Employee {
  id: number;
  employeeId: string;
  name: string;
  email: string;
  phone: string;
  position: string;
  salary: number;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

interface InsertEmployee {
  employeeId?: string;
  name: string;
  email: string;
  phone: string;
  position: string;
  salary: number;
  isActive?: boolean;
}

interface AttendanceRecord {
  id: number;
  employeeId: number;
  clockIn: Date;
  clockOut: Date | null;
  breakStart: Date | null;
  breakEnd: Date | null;
  status: string;
  notes: string | null;
  totalHours: string | null;
  overtime: string | null;
}

interface Table {
  id: number;
  tableNumber: string;
  status: string;
  capacity: number;
  createdAt: string;
  updatedAt: string;
}

interface InsertTable {
  tableNumber: string;
  status?: string;
  capacity?: number;
}

interface Order {
  id: number;
  orderNumber: string;
  tableId: number | null;
  employeeId: number | null;
  status: string;
  customerName: string | null;
  customerCount: number | null;
  subtotal: string;
  tax: string;
  discount: string;
  total: string;
  paymentMethod: string | null;
  paymentStatus: string;
  einvoiceStatus: number;
  salesChannel: string;
  notes: string | null;
  paidAt: Date | null;
  servedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  invoiceNumber?: string | null;
  invoiceId?: number | null;
  templateNumber?: string | null;
  symbol?: string | null;
  priceIncludeTax?: boolean; // Added for clarity
}

interface InsertOrder {
  orderNumber?: string;
  tableId?: number | null;
  employeeId?: number | null;
  status: string;
  customerName?: string | null;
  customerCount?: number | null;
  subtotal: number;
  tax: number;
  discount: number;
  total: number;
  paymentMethod?: string | null;
  paymentStatus?: string;
  einvoiceStatus?: number;
  salesChannel?: string;
  notes?: string | null;
  paidAt?: Date | null;
  servedAt?: Date | null;
  invoiceNumber?: string | null;
  invoiceId?: number | null;
  templateNumber?: string | null;
  symbol?: string | null;
  priceIncludeTax?: boolean; // Added for clarity
}

interface OrderItem {
  id: number;
  orderId: number;
  productId: number;
  quantity: number;
  unitPrice: number;
  total: number;
  discount?: string; // Added discount field
  notes: string | null;
  productName?: string;
  productSku?: string;
}

interface InsertOrderItem {
  productId: number;
  quantity: number;
  unitPrice: number;
  total: number;
  discount?: string; // Added discount field
  notes?: string | null;
}

interface StoreSettings {
  id: number;
  storeName: string;
  storeCode: string;
  businessType: string;
  openTime: string;
  closeTime: string;
  logoUrl?: string | null;
  currency?: string;
  taxRate?: string;
  goldThreshold?: string;
  vipThreshold?: string;
  priceIncludesTax?: boolean; // Added for clarity
  createdAt: string;
  updatedAt: string;
  defaultFloor?: string; // Added for default floor
  defaultZone?: string; // Added for default zone
  floorPrefix?: string; // Added for floor prefix
  zonePrefix?: string; // Added for zone prefix
}

interface InsertStoreSettings {
  storeName?: string;
  storeCode?: string;
  businessType?: string;
  openTime?: string;
  closeTime?: string;
  logoUrl?: string | null;
  currency?: string;
  taxRate?: string;
  goldThreshold?: string;
  vipThreshold?: string;
  priceIncludesTax?: boolean; // Added for clarity
  defaultFloor?: string; // Added for default floor
  defaultZone?: string; // Added for default zone
  floorPrefix?: string; // Added for floor prefix
  zonePrefix?: string; // Added for zone prefix
}

interface Supplier {
  id: number;
  name: string;
  code: string;
  contactPerson: string;
  phone: string;
  email: string;
  address: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}

interface InsertSupplier {
  name: string;
  code: string;
  contactPerson?: string;
  phone?: string;
  email?: string;
  address?: string;
  status?: string;
}

interface Customer {
  id: number;
  customerId: string;
  name: string;
  email: string | null;
  phone: string | null;
  address: string | null;
  visitCount: number;
  totalSpent: string;
  points: number;
  membershipLevel: string;
  createdAt: string;
  updatedAt: string;
}

interface InsertCustomer {
  customerId?: string;
  name: string;
  email?: string | null;
  phone?: string | null;
  address?: string | null;
  visitCount?: number;
  totalSpent?: string;
  points?: number;
  membershipLevel?: string;
}

interface PointTransaction {
  id: number;
  customerId: number;
  type: "earned" | "redeemed" | "adjusted";
  points: number;
  description: string;
  orderId: number | null;
  employeeId: number | null;
  previousBalance: number;
  newBalance: number;
  createdAt: string;
}

interface Invoice {
  id: number;
  invoiceNumber: string | null;
  templateNumber: string | null;
  symbol: string | null;
  customerName: string;
  customerTaxCode: string | null;
  customerAddress: string | null;
  customerPhone: string | null;
  customerEmail: string | null;
  subtotal: number;
  tax: number;
  total: number;
  paymentMethod: number;
  invoiceDate: Date;
  status: string;
  einvoiceStatus: number;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

interface InvoiceItem {
  id: number;
  invoiceId: number;
  productId: number;
  productName: string;
  quantity: number;
  unitPrice: number;
  total: number;
  taxRate: string;
}

interface InvoiceTemplate {
  id: number;
  name: string;
  templateNumber: string;
  templateCode: string | null;
  symbol: string;
  useCK: boolean;
  notes: string | null;
  isDefault: boolean;
  createdAt: string;
  updatedAt: string;
}

interface EInvoiceConnection {
  id: number;
  symbol: string;
  taxCode: string;
  loginId: string;
  password: string;
  softwareName: string;
  loginUrl?: string;
  signMethod: string;
  cqtCode: string;
  notes?: string;
  isDefault: boolean;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

interface PrinterConfig {
  id: number;
  name: string;
  printerType: string;
  connectionType: string;
  ipAddress?: string;
  port?: number;
  macAddress?: string;
  paperWidth: number;
  printSpeed: number;
  isPrimary: boolean;
  isSecondary: boolean;
  isActive: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export class DatabaseStorage implements IStorage {
  // Store db connection for reuse
  private db: any;
  private tenant: string | null = null; // Property to store tenant context if needed
  private dbManager: any;

  constructor() {
    this.db = db;
    this.dbManager = dbManager;
  }

  // Expense Voucher methods
  private async getExpenseVouchers(tenantDb?: any): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getExpenseVouchers");
    try {
      const result = await database.execute(sql`
        SELECT * FROM expense_vouchers 
        ORDER BY created_at DESC
      `);
      return result.rows || [];
    } catch (error) {
      console.error("Error fetching expense vouchers:", error);
      return [];
    }
  }

  private async createExpenseVoucher(
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createExpenseVoucher");
    try {
      const result = await database.execute(sql`
        INSERT INTO expense_vouchers (
          voucher_number, date, amount, account, recipient, 
          receiver_name, phone, category, description, 
          created_at, updated_at
        ) VALUES (
          ${voucherData.voucherNumber}, 
          ${voucherData.date}, 
          ${voucherData.amount}, 
          ${voucherData.account}, 
          ${voucherData.recipient},
          ${voucherData.receiverName || null}, 
          ${voucherData.phone || null}, 
          ${voucherData.category}, 
          ${voucherData.description || null},
          ${new Date()}, 
          ${new Date()}
        ) RETURNING *
      `);
      return result.rows[0];
    } catch (error) {
      console.error("Error creating expense voucher:", error);
      throw error;
    }
  }

  private async updateExpenseVoucher(
    id: string,
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateExpenseVoucher");
    try {
      console.log("Storage: Updating expense voucher with data:", voucherData);

      const [voucher] = await database
        .update(expenseVouchers)
        .set({
          voucherNumber: voucherData.voucherNumber,
          date: voucherData.date,
          amount: voucherData.amount.toString(), // Ensure amount is string
          account: voucherData.account,
          recipient: voucherData.recipient,
          phone: voucherData.phone || null,
          category: voucherData.category,
          description: voucherData.description || null,
          updatedAt: new Date(), // Always use new Date() for updatedAt
        })
        .where(eq(expenseVouchers.id, parseInt(id)))
        .returning();

      console.log("Storage: Expense voucher updated successfully:", voucher);
      return voucher;
    } catch (error) {
      console.error("Storage: Error updating expense voucher:", error);
      throw error;
    }
  }

  private async deleteExpenseVoucher(
    id: string,
    tenantDb?: any,
  ): Promise<void> {
    const database = tenantDb || this.getSafeDatabase("deleteExpenseVoucher");
    try {
      await database.execute(sql`
        DELETE FROM expense_vouchers WHERE id = ${id}
      `);
    } catch (error) {
      console.error("Error deleting expense voucher:", error);
      throw error;
    }
  }

  async getExpenseVouchers(tenantDb?: any): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getExpenseVouchers");
    try {
      const result = await database.execute(sql`
        SELECT * FROM expense_vouchers 
        ORDER BY created_at DESC
      `);
      return result.rows || [];
    } catch (error) {
      console.error("Error fetching expense vouchers:", error);
      return [];
    }
  }

  async createExpenseVoucher(voucherData: any, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createExpenseVoucher");
    try {
      const result = await database.execute(sql`
        INSERT INTO expense_vouchers (
          voucher_number, date, amount, account, recipient, 
          receiver_name, phone, category, description, 
          created_at, updated_at
        ) VALUES (
          ${voucherData.voucherNumber}, 
          ${voucherData.date}, 
          ${voucherData.amount}, 
          ${voucherData.account}, 
          ${voucherData.recipient},
          ${voucherData.receiverName || null}, 
          ${voucherData.phone || null}, 
          ${voucherData.category}, 
          ${voucherData.description || null},
          ${new Date()}, 
          ${new Date()}
        ) RETURNING *
      `);
      return result.rows[0];
    } catch (error) {
      console.error("Error creating expense voucher:", error);
      throw error;
    }
  }

  async updateExpenseVoucher(
    id: string,
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    try {
      const database = tenantDb || this.getSafeDatabase("updateExpenseVoucher");

      console.log("Storage: Updating expense voucher with data:", voucherData);

      const [voucher] = await database
        .update(expenseVouchers)
        .set({
          voucherNumber: voucherData.voucherNumber,
          date: voucherData.date,
          amount: voucherData.amount.toString(), // Ensure amount is string
          account: voucherData.account,
          recipient: voucherData.recipient,
          phone: voucherData.phone || null,
          category: voucherData.category,
          description: voucherData.description || null,
          updatedAt: new Date(), // Always use new Date() for updatedAt
        })
        .where(eq(expenseVouchers.id, parseInt(id)))
        .returning();

      console.log("Storage: Expense voucher updated successfully:", voucher);
      return voucher;
    } catch (error) {
      console.error("Storage: Error updating expense voucher:", error);
      throw error;
    }
  }

  async deleteExpenseVoucher(id: string, tenantDb?: any): Promise<void> {
    try {
      const database = tenantDb || this.getSafeDatabase("deleteExpenseVoucher");
      await database
        .delete(expenseVouchers)
        .where(eq(expenseVouchers.id, parseInt(id)));
    } catch (error) {
      console.error("Error deleting expense voucher:", error);
      throw error;
    }
  }

  async checkReceiptNumberExists(receiptNumber: string, excludeId?: number, tenantDb?: any): Promise<boolean> {
    try {
      const database = tenantDb || this.getSafeDatabase("checkReceiptNumberExists");

      let conditions = [eq(purchaseReceipts.receiptNumber, receiptNumber)];

      // Exclude specific ID if provided (for updates)
      if (excludeId) {
        conditions.push(ne(purchaseReceipts.id, excludeId));
      }

      const result = await database
        .select({ id: purchaseReceipts.id })
        .from(purchaseReceipts)
        .where(and(...conditions))
        .limit(1);

      return result.length > 0;
    } catch (error) {
      console.error("Error checking receipt number:", error);
      return false;
    }
  }

  // Get safe database connection with tenant support and fallback
  private getSafeDatabase(operation: string, tenantSubdomain?: string): any {
    console.log(
      `🔍 Getting safe database for operation: ${operation}${tenantSubdomain ? ` (tenant: ${tenantSubdomain})` : ""}`,
    );

    let database;

    // Use tenant-specific database if tenant is provided
    if (tenantSubdomain) {
      database = this.dbManager.getTenantDatabase(tenantSubdomain);
      console.log(`🏢 Using tenant database for ${tenantSubdomain}`);
    } else {
      database = this.db; // Default to global db
      console.log(`🏠 Using default database`);
    }

    if (!database) {
      console.error(
        `❌ CRITICAL: No database connection available for ${operation}`,
      );
      throw new Error(
        `Database connection is completely unavailable for ${operation}`,
      );
    }

    // Comprehensive validation of database object
    if (typeof database !== "object" || database === null) {
      console.error(`❌ Database is not a valid object in ${operation}:`, {
        type: typeof database,
        isNull: database === null,
        isUndefined: database === undefined,
      });
      throw new Error(`Invalid database connection for ${operation}`);
    }

    // Validate required methods exist
    const requiredMethods = ["select", "insert", "update", "delete"];
    const missingMethods = requiredMethods.filter(
      (method) => !database[method] || typeof database[method] !== "function",
    );

    if (missingMethods.length > 0) {
      console.error(`od Database missing required methods in ${operation}:`, {
        missingMethods,
        availableMethods: Object.keys(database).filter(
          (key) => typeof database[key] === "function",
        ),
      });
      throw new Error(
        `Database connection is invalid - missing methods: ${missingMethods.join(", ")} for ${operation}`,
      );
    }

    console.log(
      `✅ Database validation passed for ${operation}${tenantSubdomain ? ` (tenant: ${tenantSubdomain})` : ""}`,
    );
    return database;
  }

  async getCategories(tenantDb?: any): Promise<Category[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getCategories");
      const result = await database.select().from(categories);
      return result || [];
    } catch (error) {
      console.error(`❌ Error in getCategories:`, error);
      // Return empty array instead of throwing to prevent crashes
      return [];
    }
  }

  async createCategory(
    insertCategory: InsertCategory,
    tenantDb?: any,
  ): Promise<Category> {
    const database = tenantDb || this.getSafeDatabase("createCategory");
    const [category] = await database
      .insert(categories)
      .values(insertCategory)
      .returning();
    return category;
  }

  async updateCategory(
    id: number,
    updateData: Partial<InsertCategory>,
    tenantDb?: any,
  ): Promise<Category> {
    const database = tenantDb || this.getSafeDatabase("updateCategory");
    const [category] = await database
      .update(categories)
      .set(updateData)
      .where(eq(categories.id, id))
      .returning();
    return category;
  }

  async deleteCategory(id: number, tenantDb?: any): Promise<void> {
    const database = tenantDb || this.getSafeDatabase("deleteCategory");
    await database.delete(categories).where(eq(categories.id, id));
  }

  async getProducts(tenantDb?: any): Promise<Product[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getProducts");
      const result = await database
        .select()
        .from(products)
        .where(eq(products.isActive, true));
      // Ensure productType has a default value if missing and afterTaxPrice is properly returned
      return result.map((product) => ({
        ...product,
        productType: product.productType || 1,
        afterTaxPrice: product.afterTaxPrice || null,
        beforeTaxPrice: product.beforeTaxPrice || null,
        floor: product.floor || "1", // Default floor
      }));
    } catch (error) {
      console.error(`❌ Error in getProducts:`, error);
      return [];
    }
  }

  async getProductsByCategory(
    categoryId: number,
    includeInactive: boolean = false,
    tenantDb?: any,
  ): Promise<Product[]> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("getProductsByCategory");
      let whereCondition = eq(products.categoryId, categoryId);

      if (!includeInactive) {
        whereCondition = and(whereCondition, eq(products.isActive, true));
      }

      const result = await database
        .select()
        .from(products)
        .where(whereCondition)
        .orderBy(products.name);

      // Ensure afterTaxPrice and beforeTaxPrice are properly returned
      return result.map((product) => ({
        ...product,
        afterTaxPrice: product.afterTaxPrice || null,
        beforeTaxPrice: product.beforeTaxPrice || null,
        floor: product.floor || "1", // Default floor
      }));
    } catch (error) {
      console.error(`❌ Error in getProductsByCategory:`, error);
      return [];
    }
  }

  async getProduct(id: number, tenantDb?: any): Promise<Product | undefined> {
    const database = tenantDb || this.getSafeDatabase("getProduct");
    const [product] = await database
      .select()
      .from(products)
      .where(and(eq(products.id, id), eq(products.isActive, true)));
    // Ensure default floor if missing
    if (product) {
      product.floor = product.floor || "1";
    }
    return product || undefined;
  }

  async getProductBySku(
    sku: string,
    tenantDb?: any,
  ): Promise<Product | undefined> {
    const database = tenantDb || this.getSafeDatabase("getProductBySku");
    const [product] = await database
      .select()
      .from(products)
      .where(and(eq(products.sku, sku), eq(products.isActive, true)));
    // Ensure default floor if missing
    if (product) {
      product.floor = product.floor || "1";
    }
    return product || undefined;
  }

  async searchProducts(
    query: string,
    includeInactive: boolean = false,
    tenantDb?: any,
  ): Promise<Product[]> {
    const database = tenantDb || this.getSafeDatabase("searchProducts");
    let whereCondition = or(
      ilike(products.name, `%${query}%`),
      ilike(products.sku, `%${query}%`),
    );

    if (!includeInactive) {
      whereCondition = and(whereCondition, eq(products.isActive, true));
    }

    const results = await database
      .select()
      .from(products)
      .where(whereCondition);
    // Ensure default floor if missing
    return results.map((product) => ({
      ...product,
      floor: product.floor || "1",
    }));
  }

  async createProduct(
    insertProduct: InsertProduct,
    tenantDb?: any,
  ): Promise<Product> {
    const database = tenantDb || this.getSafeDatabase("createProduct");
    try {
      console.log("Storage: Creating product with data:", insertProduct);
      console.log(
        "Storage: PriceIncludesTax value:",
        insertProduct.priceIncludesTax,
        typeof insertProduct.priceIncludesTax,
      );

      // Fetch store settings to determine tax rate and priceIncludeTax logic
      const storeSettings = await this.getStoreSettings(tenantDb);

      // Use the taxRate provided in insertProduct if available, otherwise default to storeSettings.taxRate or "10.00"
      const explicitTaxRate = insertProduct.taxRate; // This is the input value, e.g., "10.00"
      const taxRate = parseFloat(
        explicitTaxRate || storeSettings?.taxRate || "0",
      );

      console.log("Storage: Tax rate preservation debug:", {
        inputTaxRate: insertProduct.taxRate,
        parsedTaxRate: taxRate,
        willUseValue: explicitTaxRate || storeSettings?.taxRate || "0",
      });

      const priceIncludesTax =
        insertProduct.priceIncludesTax !== undefined
          ? insertProduct.priceIncludesTax
          : storeSettings?.priceIncludesTax || false;

      let beforeTaxPrice = insertProduct.price;
      if (priceIncludesTax) {
        if (taxRate > 0) {
          beforeTaxPrice = insertProduct.price / (1 + taxRate / 100);
        }
      }

      const productData = {
        name: insertProduct.name,
        sku: insertProduct.sku,
        price: insertProduct.price, // This will be the price_with_tax if priceIncludesTax is true
        stock: insertProduct.stock,
        categoryId: insertProduct.categoryId,
        productType: insertProduct.productType || 1,
        trackInventory: insertProduct.trackInventory !== false,
        imageUrl: insertProduct.imageUrl || null,
        isActive: true,
        priceIncludesTax: priceIncludesTax,
        beforeTaxPrice: beforeTaxPrice,
        afterTaxPrice: priceIncludesTax
          ? insertProduct.price
          : beforeTaxPrice * (1 + taxRate / 100), // Ensure afterTaxPrice is set correctly
        taxRate: explicitTaxRate || storeSettings?.taxRate || "0", // Use the explicit tax rate from input
        floor: insertProduct.floor || storeSettings?.defaultFloor || "1", // Use provided floor or default from store settings
      };

      console.log("Storage: PriceIncludesTax save debug:", {
        inputValue: insertProduct.priceIncludesTax,
        inputType: typeof insertProduct.priceIncludesTax,
        savedValue: productData.priceIncludesTax,
        savedType: typeof productData.priceIncludesTax,
      });

      console.log("Storage: Inserting product data:", productData);

      const [product] = await database
        .insert(products)
        .values(productData)
        .returning();

      console.log("Storage: Product created successfully:", product);
      console.log(
        "Created product priceIncludesTax:",
        product.priceIncludesTax,
      );
      return product;
    } catch (error: any) {
      console.error("Storage: Error creating product:", error);

      // Handle duplicate key error by fixing sequence
      if (error?.code === "23505" && error?.constraint === "products_pkey") {
        console.log(
          "🔧 Fixing products sequence due to duplicate key error...",
        );
        try {
          // Get max ID from products table using Drizzle SQL
          const maxIdResult = await database.execute(
            sql`SELECT COALESCE(MAX(id), 0) as max_id FROM products`,
          );
          const maxId = maxIdResult.rows[0]?.max_id || 0;
          const newSeqValue = maxId + 100; // Set sequence well above current max

          console.log(
            `📊 Current max ID: ${maxId}, setting sequence to: ${newSeqValue}`,
          );

          // Reset sequence using Drizzle SQL
          await database.execute(
            sql`SELECT setval('products_id_seq', ${newSeqValue}, true)`,
          );
          console.log(`✅ Products sequence reset to ${newSeqValue}`);

          // Retry the insert
          console.log("🔄 Retrying product creation...");
          const [product] = await database
            .insert(products)
            .values(productData) // Make sure productData is accessible here
            .returning();

          console.log("✅ Product created successfully on retry:", product);
          return product;
        } catch (retryError) {
          console.error("❌ Failed to fix sequence and retry:", retryError);
          // Even if sequence fix fails, try with a random high ID
          try {
            console.log("🎲 Attempting with random high ID...");
            const randomId = new Date(); // Use timestamp as ID
            const productWithId = { ...productData, id: randomId }; // Make sure productData is accessible here
            const [product] = await database
              .insert(products)
              .values(productWithId)
              .returning();
            console.log("✅ Product created with random ID:", product);
            return product;
          } catch (finalError) {
            console.error("❌ All retry attempts failed:", finalError);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  async updateProduct(
    id: number,
    updateData: Partial<InsertProduct>,
    tenantDb?: any,
  ): Promise<Product | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateProduct");
    try {
      console.log("Updating product with data:", updateData);
      console.log(
        "PriceIncludesTax value:",
        updateData.priceIncludesTax,
        typeof updateData.priceIncludesTax,
      );

      // Fetch store settings to determine default tax rate and priceIncludesTax logic
      const storeSettings = await this.getStoreSettings(tenantDb);

      // Use the taxRate provided in updateData if available, otherwise from storeSettings, default to "10.00"
      const explicitTaxRate = updateData.taxRate; // This is the input value, e.g., "10.00"
      const taxRate = parseFloat(
        explicitTaxRate || storeSettings?.taxRate || "0",
      );

      console.log("Storage: Tax rate preservation debug:", {
        inputTaxRate: updateData.taxRate,
        parsedTaxRate: taxRate,
        willUseValue: explicitTaxRate || storeSettings?.taxRate || "0",
      });

      // If priceIncludesTax is not provided in updateData, use store setting
      const priceIncludesTax =
        updateData.priceIncludesTax !== undefined
          ? updateData.priceIncludesTax
          : storeSettings?.priceIncludesTax || false;

      let beforeTaxPrice;
      let afterTaxPrice;

      if (priceIncludesTax) {
        // If store setting is true: prices include tax
        if (taxRate > 0) {
          beforeTaxPrice = updateData.price
            ? updateData.price / (1 + taxRate / 100)
            : 0;
        } else {
          beforeTaxPrice = updateData.price || 0;
        }
        afterTaxPrice = updateData.price || 0; // The provided price is the after-tax price
      } else {
        // If store setting is false: prices exclude tax
        beforeTaxPrice = updateData.price || 0; // The provided price is the before-tax price
        afterTaxPrice = updateData.price
          ? updateData.price * (1 + taxRate / 100)
          : 0;
      }

      // Ensure the price field in the database accurately reflects the price_with_tax if priceIncludesTax is true
      const finalPrice = priceIncludesTax ? afterTaxPrice : beforeTaxPrice;

      const processedUpdates = {
        ...updateData,
        price: updateData.price !== undefined ? finalPrice : undefined, // Only update if price is provided
        priceIncludesTax: priceIncludesTax,
        beforeTaxPrice: beforeTaxPrice,
        afterTaxPrice: afterTaxPrice,
        taxRate: explicitTaxRate || storeSettings?.taxRate || "0", // Use the explicit tax rate from input
        imageUrl:
          updateData.imageUrl === undefined
            ? undefined
            : updateData.imageUrl || null, // Ensure imageUrl is handled
        floor: updateData.floor || storeSettings?.defaultFloor || "1", // Use provided floor or default from store settings
      };

      // Remove undefined values to avoid overwriting existing fields with undefined
      Object.keys(processedUpdates).forEach((key) => {
        if (processedUpdates[key] === undefined) {
          delete processedUpdates[key];
        }
      });

      console.log("Storage: Update PriceIncludesTax debug:", {
        inputValue: updateData.priceIncludesTax,
        inputType: typeof updateData.priceIncludesTax,
        processedValue: processedUpdates.priceIncludesTax,
        processedType: typeof processedUpdates.priceIncludesTax,
      });

      const [product] = await database
        .update(products)
        .set(processedUpdates)
        .where(and(eq(products.id, id), eq(products.isActive, true)))
        .returning();

      console.log("Product updated:", product);
      console.log(
        "Updated product priceIncludesTax:",
        product?.priceIncludesTax,
      );
      return product || undefined;
    } catch (error) {
      console.error("Error updating product:", error);
      throw error;
    }
  }

  async deleteProduct(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteProduct");
    try {
      // Check if product exists in transactions
      const transactionItemsCheck = await database
        .select()
        .from(transactionItems)
        .where(eq(transactionItems.productId, id))
        .limit(1);

      if (transactionItemsCheck.length > 0) {
        throw new Error(
          "Cannot delete product: it has been used in transactions",
        );
      }

      // Check if product exists in order items
      const orderItemsCheck = await database
        .select()
        .from(orderItemsTable)
        .where(eq(orderItemsTable.productId, id))
        .limit(1);

      if (orderItemsCheck.length > 0) {
        throw new Error("Cannot delete product: it has been used in orders");
      }

      // If no references found, delete the product
      const result = await database
        .delete(products)
        .where(eq(products.id, id))
        .returning();

      return result.length > 0;
    } catch (error) {
      console.error("Error deleting product:", error);
      throw error;
    }
  }

  async deleteInactiveProducts(tenantDb?: any): Promise<number> {
    const database = tenantDb || this.getSafeDatabase("deleteInactiveProducts");
    const result = await database
      .delete(products)
      .where(eq(products.isActive, false))
      .returning();
    return result.length;
  }

  async updateProductStock(
    id: number,
    quantity: number,
    tenantDb?: any,
  ): Promise<Product | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateProductStock");

    try {
      console.log(
        `🔍 Starting stock update for product ID: ${id}, quantity change: ${quantity}`,
      );

      const [product] = await database
        .select()
        .from(products)
        .where(eq(products.id, id));

      if (!product) {
        console.error(`❌ Product not found for stock update: ID ${id}`);
        throw new Error(`Product with ID ${id} not found`);
      }

      console.log(
        `📋 Product found: ${product.name}, current stock: ${product.stock}, tracks inventory: ${product.trackInventory}`,
      );

      // Check if product tracks inventory before updating
      if (!product.trackInventory) {
        console.log(
          `⏭️ Product ${product.name} does not track inventory - skipping stock update`,
        );
        return product; // Return the original product without updating stock
      }

      const currentStock = product.stock || 0;
      // Đơn giản: chỉ cần lấy tổng tồn kho hiện tại trừ đi số lượng bán
      const newStock = currentStock - Math.abs(quantity);

      // Log the stock calculation
      console.log(
        `📦 Simple stock calculation for ${product.name} (ID: ${id}):`,
      );
      console.log(`   - Current stock: ${currentStock}`);
      console.log(`   - Quantity to subtract: ${Math.abs(quantity)}`);
      console.log(`   - New stock: ${newStock}`);

      // Check if we have sufficient stock
      if (newStock < 0) {
        const errorMsg = `Insufficient stock for ${product.name}. Available: ${currentStock}, Required: ${Math.abs(quantity)}`;
        console.error(`❌ ${errorMsg}`);
        throw new Error(errorMsg);
      }

      const [updatedProduct] = await database
        .update(products)
        .set({ stock: newStock })
        .where(eq(products.id, id))
        .returning();

      if (updatedProduct) {
        console.log(
          `✅ Stock updated successfully for ${product.name}: ${currentStock} → ${newStock}`,
        );

        // Create inventory transaction record
        try {
          await database.execute(sql`
            INSERT INTO inventory_transactions
            (product_id, type, quantity, previous_stock, new_stock, notes, created_at)
            VALUES (${id}, 'subtract', ${Math.abs(quantity)}, ${currentStock}, ${newStock},
                   'Stock deduction from sale', ${new Date()})
          `);
          console.log(`📝 Inventory transaction recorded for ${product.name}`);
        } catch (invError) {
          console.error(`❌ Failed to record inventory transaction:`, invError);
          // Don't throw here as the stock update was successful
        }

        return updatedProduct;
      } else {
        console.error(
          `❌ Failed to update stock for ${product.name} - no updated product returned`,
        );
        throw new Error(`Failed to update stock for product: ${product.name}`);
      }
    } catch (error) {
      console.error(`❌ Error updating stock for product ID ${id}:`, error);
      throw error; // Re-throw the error so the caller can handle it
    }
  }

  async createTransaction(
    insertTransaction: InsertTransaction,
    items: InsertTransactionItem[],
    tenantDb?: any,
  ): Promise<Receipt> {
    const database = tenantDb || this.getSafeDatabase("createTransaction");
    console.log(`🔄 Creating transaction: ${insertTransaction.transactionId}`);
    console.log(`📦 Processing ${items.length} items for inventory deduction`);
    console.log(
      `📋 Transaction details:`,
      JSON.stringify(insertTransaction, null, 2),
    );

    try {
      // Create the main transaction record
      const [transaction] = await database
        .insert(transactions)
        .values({
          ...insertTransaction,
          amountReceived: insertTransaction.amountReceived || null,
          change: insertTransaction.change || null,
        })
        .returning();

      console.log(`✅ Transaction record created with ID: ${transaction.id}`);

      const transactionItemsWithIds: TransactionItem[] = [];
      const stockUpdateResults: Array<{
        productName: string;
        success: boolean;
        error?: string;
      }> = [];

      // Process each item
      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        console.log(
          `📝 Processing item ${i + 1}/${items.length}: ${item.productName} (ID: ${item.productId}) - Qty: ${item.quantity}`,
        );

        try {
          // Create transaction item record
          const [transactionItem] = await database
            .insert(transactionItems)
            .values({
              ...item,
              transactionId: transaction.id,
            })
            .returning();

          console.log(
            `✅ Transaction item created with ID: ${transactionItem.id}`,
          );

          // Update product stock - trừ tồn kho đơn giản
          console.log(
            `🔢 Updating stock for product ID ${item.productId}: subtract ${item.quantity}`,
          );

          try {
            const updatedProduct = await this.updateProductStock(
              item.productId,
              item.quantity,
              tenantDb,
            );

            if (updatedProduct) {
              console.log(
                `✅ Stock successfully updated for ${item.productName}: New stock = ${updatedProduct.stock}`,
              );
              stockUpdateResults.push({
                productName: item.productName,
                success: true,
              });
            } else {
              const errorMsg = `Failed to update stock for ${item.productName} - no product returned`;
              console.error(`❌ ${errorMsg}`);
              stockUpdateResults.push({
                productName: item.productName,
                success: false,
                error: errorMsg,
              });
            }
          } catch (stockError) {
            const errorMsg =
              stockError instanceof Error
                ? stockError.message
                : String(stockError);
            console.error(
              `❌ Stock update error for ${item.productName}:`,
              errorMsg,
            );
            stockUpdateResults.push({
              productName: item.productName,
              success: false,
              error: errorMsg,
            });
          }

          transactionItemsWithIds.push(transactionItem);
        } catch (itemError) {
          console.error(
            `❌ Error processing transaction item ${item.productName}:`,
            itemError,
          );
          throw new Error(
            `Failed to process item ${item.productName}: ${itemError instanceof Error ? itemError.message : String(itemError)}`,
          );
        }
      }

      // Log stock update summary
      const successfulUpdates = stockUpdateResults.filter((r) => r.success);
      const failedUpdates = stockUpdateResults.filter((r) => !r.success);

      console.log(`📊 Stock update summary:`);
      console.log(
        `   - Successful: ${successfulUpdates.length}/${items.length}`,
      );
      console.log(`   - Failed: ${failedUpdates.length}/${items.length}`);

      if (failedUpdates.length > 0) {
        console.error(`❌ Failed stock updates:`, failedUpdates);
        // Log but don't fail the transaction - the transaction was created successfully
      }

      console.log(
        `✅ Transaction created successfully: ${transaction.transactionId} with ${transactionItemsWithIds.length} items`,
      );

      return {
        ...transaction,
        items: transactionItemsWithIds,
      };
    } catch (error) {
      console.error(
        `❌ Error creating transaction ${insertTransaction.transactionId}:`,
        error,
      );
      throw new Error(
        `Failed to create transaction: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  async getTransaction(
    id: number,
    tenantDb?: any,
  ): Promise<Receipt | undefined> {
    const database = tenantDb || this.getSafeDatabase("getTransaction");
    const [transaction] = await database
      .select()
      .from(transactions)
      .where(eq(transactions.id, id));

    if (!transaction) return undefined;

    const items = await database
      .select()
      .from(transactionItems)
      .where(eq(transactionItems.transactionId, id));

    return { ...transaction, items };
  }

  async getTransactionByTransactionId(
    transactionId: string,
    tenantDb?: any,
  ): Promise<Receipt | undefined> {
    const database =
      tenantDb || this.getSafeDatabase("getTransactionByTransactionId");
    const [transaction] = await database
      .select()
      .from(transactions)
      .where(eq(transactions.transactionId, transactionId));

    if (!transaction) return undefined;

    const items = await database
      .select()
      .from(transactionItems)
      .where(eq(transactionItems.transactionId, transaction.id));

    return { ...transaction, items };
  }

  async getTransactions(tenantDb?: any): Promise<Transaction[]> {
    const database = tenantDb || this.getSafeDatabase("getTransactions");
    return await database
      .select()
      .from(transactions)
      .orderBy(transactions.createdAt);
  }

  // Get next employee ID in sequence
  async getNextEmployeeId(tenantDb?: any): Promise<string> {
    const database = tenantDb || this.getSafeDatabase("getNextEmployeeId");
    try {
      const lastEmployee = await database
        .select()
        .from(employees)
        .orderBy(desc(employees.id))
        .limit(1);

      if (lastEmployee.length === 0) {
        return "EMP-001";
      }

      // Extract number from last employee ID (EMP-001 -> 001)
      const lastId = lastEmployee[0].employeeId;
      const match = lastId.match(/EMP-(\d+)/);

      if (match) {
        const lastNumber = parseInt(match[1], 10);
        const nextNumber = lastNumber + 1;
        return `EMP-${nextNumber.toString().padStart(3, "0")}`;
      }

      // Fallback if format doesn't match
      return "EMP-001";
    } catch (error) {
      console.error("Error generating next employee ID:", error);
      return "EMP-001";
    }
  }

  // Generate next customer ID
  async getNextCustomerId(tenantDb?: any): Promise<string> {
    const database = tenantDb || this.getSafeDatabase("getNextCustomerId");
    try {
      // Get all customer IDs that match the CUST pattern and extract numbers
      const allCustomers = await database
        .select({ customerId: customers.customerId })
        .from(customers)
        .where(like(customers.customerId, "CUST%"));

      if (allCustomers.length === 0) {
        return "CUST001";
      }

      // Extract all numbers from existing customer IDs
      const existingNumbers = allCustomers
        .map((customer) => {
          const match = customer.customerId.match(/CUST(\d+)/);
          return match ? parseInt(match[1], 10) : 0;
        })
        .filter((num) => num > 0)
        .sort((a, b) => b - a); // Sort descending

      // Find the highest number and increment
      const highestNumber = existingNumbers[0] || 0;
      const nextNumber = highestNumber + 1;

      return `CUST${nextNumber.toString().padStart(3, "0")}`;
    } catch (error) {
      console.error("Error generating next customer ID:", error);
      return "CUST001";
    }
  }

  // Employee methods
  async getEmployees(tenantDb?: any): Promise<Employee[]> {
    const database = tenantDb || this.getSafeDatabase("getEmployees");
    return await database
      .select()
      .from(employees)
      .where(eq(employees.isActive, true));
  }

  async getEmployee(id: number, tenantDb?: any): Promise<Employee | undefined> {
    const database = tenantDb || this.getSafeDatabase("getEmployee");
    const [employee] = await database
      .select()
      .from(employees)
      .where(and(eq(employees.id, id), eq(employees.isActive, true)));
    return employee || undefined;
  }

  async getEmployeeByEmployeeId(
    employeeId: string,
    tenantDb?: any,
  ): Promise<Employee | undefined> {
    const database =
      tenantDb || this.getSafeDatabase("getEmployeeByEmployeeId");
    const [employee] = await database
      .select()
      .from(employees)
      .where(
        and(eq(employees.employeeId, employeeId), eq(employees.isActive, true)),
      );
    return employee || undefined;
  }

  async createEmployee(
    insertEmployee: InsertEmployee,
    tenantDb?: any,
  ): Promise<Employee> {
    const database = tenantDb || this.getSafeDatabase("createEmployee");
    const [employee] = await database
      .insert(employees)
      .values(insertEmployee)
      .returning();
    return employee;
  }

  async updateEmployee(
    id: number,
    updateData: Partial<InsertEmployee>,
    tenantDb?: any,
  ): Promise<Employee | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateEmployee");
    const [employee] = await database
      .update(employees)
      .set(updateData)
      .where(and(eq(employees.id, id), eq(employees.isActive, true)))
      .returning();
    return employee || undefined;
  }

  async deleteEmployee(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteEmployee");
    try {
      // Check if employee has attendance records
      const attendanceCheck = await database
        .select()
        .from(attendanceRecords)
        .where(eq(attendanceRecords.employeeId, id))
        .limit(1);

      if (attendanceCheck.length > 0) {
        throw new Error(
          "Cannot delete employee: employee has attendance records",
        );
      }

      // Check if employee has orders
      const orderCheck = await database
        .select()
        .from(orders)
        .where(eq(orders.employeeId, id))
        .limit(1);

      if (orderCheck.length > 0) {
        throw new Error("Cannot delete employee: employee has orders");
      }

      // If no references found, delete the employee
      const result = await database
        .delete(employees)
        .where(eq(employees.id, id))
        .returning();

      return result.length > 0;
    } catch (error) {
      console.error("Error deleting employee:", error);
      throw error;
    }
  }

  async getAttendanceRecords(
    employeeId?: number,
    date?: string,
    tenantDb?: any,
  ): Promise<AttendanceRecord[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getAttendanceRecords");

      const conditions = [];

      if (employeeId) {
        conditions.push(eq(attendanceRecords.employeeId, employeeId));
      }

      if (date) {
        const startDate = new Date(date);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(date);
        endDate.setHours(23, 59, 59, 999);

        conditions.push(
          gte(attendanceRecords.clockIn, startDate),
          lte(attendanceRecords.clockIn, endDate),
        );
      }

      if (conditions.length > 0) {
        return await database
          .select()
          .from(attendanceRecords)
          .where(and(...conditions))
          .orderBy(attendanceRecords.clockIn);
      }

      return await database
        .select()
        .from(attendanceRecords)
        .orderBy(attendanceRecords.clockIn);
    } catch (error) {
      console.error(`❌ Error in getAttendanceRecords:`, error);
      return [];
    }
  }

  async getAttendanceRecord(
    id: number,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("getAttendanceRecord");

    try {
      const [record] = await database
        .select()
        .from(attendanceRecords)
        .where(eq(attendanceRecords.id, id));
      return record || undefined;
    } catch (error) {
      console.error(`❌ Error in getAttendanceRecord:`, error);
      return undefined;
    }
  }

  async getTodayAttendance(
    employeeId: number,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("getTodayAttendance");

    try {
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const tomorrow = new Date(today);
      tomorrow.setDate(tomorrow.getDate() + 1);

      const [record] = await database
        .select()
        .from(attendanceRecords)
        .where(
          and(
            eq(attendanceRecords.employeeId, employeeId),
            gte(attendanceRecords.clockIn, today),
            lte(attendanceRecords.clockIn, tomorrow),
          ),
        );
      return record || undefined;
    } catch (error) {
      console.error(`❌ Error in getTodayAttendance:`, error);
      return undefined;
    }
  }

  async clockIn(
    employeeId: number,
    notes?: string,
    tenantDb?: any,
  ): Promise<AttendanceRecord> {
    const database = tenantDb || this.getSafeDatabase("clockIn");
    try {
      // Check if employee exists
      const [employee] = await database
        .select()
        .from(employees)
        .where(eq(employees.id, employeeId))
        .limit(1);

      if (!employee) {
        throw new Error(`Employee with ID ${employeeId} not found`);
      }

      // Check if already clocked in today
      const existingRecord = await this.getTodayAttendance(
        employeeId,
        tenantDb,
      );
      if (existingRecord) {
        throw new Error("Employee already clocked in today");
      }

      const clockInTime = new Date();
      const [record] = await database
        .insert(attendanceRecords)
        .values({
          employeeId,
          clockIn: clockInTime,
          status: "present",
          notes: notes || null,
        })
        .returning();

      if (!record) {
        throw new Error("Failed to create attendance record");
      }

      return record;
    } catch (error) {
      console.error("Clock-in error:", error);
      throw error;
    }
  }

  async clockOut(
    attendanceId: number,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("clockOut");
    const clockOutTime = new Date();
    const record = await this.getAttendanceRecord(attendanceId, tenantDb);
    if (!record) return undefined;

    const clockInTime = new Date(record.clockIn);
    const totalMinutes =
      (clockOutTime.getTime() - clockInTime.getTime()) / (1000 * 60);
    let totalHours = totalMinutes / 60;

    // Subtract break time if any
    if (record.breakStart && record.breakEnd) {
      const breakMinutes =
        (new Date(record.breakEnd).getTime() -
          new Date(record.breakStart).getTime()) /
        (1000 * 60);
      totalHours -= breakMinutes / 60;
    }

    // Calculate overtime (assuming 8 hour work day)
    const overtime = Math.max(0, totalHours - 8);

    const [updatedRecord] = await database
      .update(attendanceRecords)
      .set({
        clockOut: clockOutTime,
        totalHours: totalHours.toFixed(2),
        overtime: overtime.toFixed(2),
      })
      .where(eq(attendanceRecords.id, attendanceId))
      .returning();

    return updatedRecord || undefined;
  }

  async startBreak(
    attendanceId: number,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("startBreak");
    const [record] = await database
      .update(attendanceRecords)
      .set({ breakStart: new Date() })
      .where(eq(attendanceRecords.id, attendanceId))
      .returning();
    return record || undefined;
  }

  async endBreak(
    attendanceId: number,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("endBreak");
    const [record] = await database
      .update(attendanceRecords)
      .set({ breakEnd: new Date() })
      .where(eq(attendanceRecords.id, attendanceId))
      .returning();
    return record || undefined;
  }

  async updateAttendanceStatus(
    id: number,
    status: string,
    tenantDb?: any,
  ): Promise<AttendanceRecord | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateAttendanceStatus");
    const [record] = await database
      .update(attendanceRecords)
      .set({ status })
      .where(eq(attendanceRecords.id, id))
      .returning();
    return record || undefined;
  }

  // Tables
  async getTables(tenantDb?: any): Promise<Table[]> {
    const database = tenantDb || this.getSafeDatabase("getTables");

    try {
      return await database.select().from(tables).orderBy(tables.tableNumber);
    } catch (error) {
      console.error(`❌ Error in getTables:`, error);
      return [];
    }
  }

  async getTable(id: number, tenantDb?: any): Promise<Table | undefined> {
    const database = tenantDb || this.getSafeDatabase("getTable");

    try {
      const [table] = await database
        .select()
        .from(tables)
        .where(eq(tables.id, id));
      return table || undefined;
    } catch (error) {
      console.error(`❌ Error in getTable:`, error);
      return undefined;
    }
  }

  async getTableByNumber(
    tableNumber: string,
    tenantDb?: any,
  ): Promise<Table | undefined> {
    const database = tenantDb || this.getSafeDatabase("getTableByNumber");

    try {
      const [table] = await database
        .select()
        .from(tables)
        .where(eq(tables.tableNumber, tableNumber));
      return table || undefined;
    } catch (error) {
      console.error(`❌ Error in getTableByNumber:`, error);
      return undefined;
    }
  }

  async createTable(table: InsertTable, tenantDb?: any): Promise<Table> {
    const database = tenantDb || this.getSafeDatabase("createTable");

    try {
      const [newTable] = await database
        .insert(tables)
        .values(table)
        .returning();
      return newTable;
    } catch (error) {
      console.error(`❌ Error in createTable:`, error);
      throw error;
    }
  }

  async updateTable(
    id: number,
    table: Partial<InsertTable>,
    tenantDb?: any,
  ): Promise<Table | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateTable");

    try {
      const [updatedTable] = await database
        .update(tables)
        .set(table)
        .where(eq(tables.id, id))
        .returning();
      return updatedTable || undefined;
    } catch (error) {
      console.error(`❌ Error in updateTable:`, error);
      return undefined;
    }
  }

  async updateTableStatus(
    id: number,
    status: string,
    tenantDb?: any,
  ): Promise<Table | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateTableStatus");

    try {
      const [table] = await database
        .update(tables)
        .set({ status })
        .where(eq(tables.id, id))
        .returning();
      return table || undefined;
    } catch (error) {
      console.error(`❌ Error in updateTableStatus:`, error);
      return undefined;
    }
  }

  async deleteTable(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteTable");

    try {
      const result = await database.delete(tables).where(eq(tables.id, id));
      return (result.rowCount ?? 0) > 0;
    } catch (error) {
      console.error(`❌ Error in deleteTable:`, error);
      return false;
    }
  }

  // Orders
  async getOrders(
    tableId?: number,
    status?: string,
    tenantDb?: any,
    salesChannel?: string,
  ): Promise<any[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getOrders");

      let query = database.select().from(orders);

      const conditions = [];

      if (tableId) {
        conditions.push(eq(orders.tableId, tableId));
      }

      if (status) {
        conditions.push(eq(orders.status, status));
      }

      if (salesChannel) {
        conditions.push(eq(orders.salesChannel, salesChannel));
      }

      if (conditions.length > 0) {
        query = query.where(and(...conditions));
      }

      const result = await query.orderBy(desc(orders.orderedAt));
      console.log(
        `Storage: getOrders returned ${result?.length || 0} orders${salesChannel ? ` for channel: ${salesChannel}` : ""}`,
      );
      return result || [];
    } catch (error) {
      console.error("Storage: getOrders error:", error);
      return [];
    }
  }

  async getOrder(id: number, tenantDb?: any): Promise<Order | undefined> {
    const database = tenantDb || this.getSafeDatabase("getOrder");

    try {
      const [order] = await database
        .select()
        .from(orders)
        .where(eq(orders.id, id));
      return order || undefined;
    } catch (error) {
      console.error(`❌ Error in getOrder:`, error);
      return undefined;
    }
  }

  async getOrderByNumber(
    orderNumber: string,
    tenantDb?: any,
  ): Promise<Order | undefined> {
    const database = tenantDb || this.getSafeDatabase("getOrderByNumber");

    try {
      const [order] = await database
        .select()
        .from(orders)
        .where(eq(orders.orderNumber, orderNumber));
      return order || undefined;
    } catch (error) {
      console.error(`❌ Error in getOrderByNumber:`, error);
      return undefined;
    }
  }

  async createOrder(
    orderData: InsertOrder,
    items: InsertOrderItem[],
    tenantDb?: any,
  ): Promise<Order> {
    const database = tenantDb || this.getSafeDatabase("createOrder");

    // Get store settings to determine price_include_tax if not provided
    let priceIncludeTax = orderData.priceIncludeTax;
    if (priceIncludeTax === undefined) {
      try {
        const [storeSettings] = await database
          .select({ priceIncludesTax: storeSettings.priceIncludesTax })
          .from(storeSettings)
          .limit(1);
        priceIncludeTax = storeSettings?.priceIncludesTax || false;
      } catch (error) {
        console.warn(
          "Could not fetch store settings, defaulting priceIncludeTax to false",
        );
        priceIncludeTax = false;
      }
    }

    console.log(`📝 Creating order with data:`, {
      orderNumber: orderData.orderNumber,
      tableId: orderData.tableId,
      subtotal: orderData.subtotal,
      tax: orderData.tax,
      total: orderData.total,
      salesChannel: orderData.salesChannel,
      priceIncludeTax: priceIncludeTax,
      itemsCount: items.length,
    });

    // Ensure salesChannel is set properly
    if (!orderData.salesChannel) {
      orderData.salesChannel = orderData.tableId ? "table" : "pos";
    }

    console.log(
      `📝 Final order data with salesChannel: ${orderData.salesChannel}`,
      orderData,
    );

    // Create the order - ensure proper field mapping (save discount without recalculating total)
    const orderInsertData = {
      orderNumber: orderData.orderNumber,
      tableId: orderData.tableId,
      employeeId: orderData.employeeId || null,
      status: orderData.status,
      customerName: orderData.customerName,
      customerCount: orderData.customerCount,
      subtotal: orderData.subtotal
        ? parseFloat(orderData.subtotal.toString())
        : 0,
      tax: orderData.tax ? parseFloat(orderData.tax.toString()) : 0,
      discount: orderData.discount
        ? parseFloat(orderData.discount.toString())
        : 0,
      total: orderData.total ? parseFloat(orderData.total.toString()) : 0,
      paymentMethod: orderData.paymentMethod,
      paymentStatus: orderData.paymentStatus,
      einvoiceStatus: orderData.einvoiceStatus || 0,
      salesChannel: orderData.salesChannel,
      notes: orderData.notes,
      paidAt: orderData.paidAt,
      orderedAt: new Date(),
    };

    console.log(`📝 Final order insert data:`, orderInsertData);

    const [order] = await database
      .insert(orders)
      .values({
        ...orderData,
        priceIncludeTax: priceIncludeTax,
        orderedAt: new Date(),
      })
      .returning();

    console.log(
      `Storage: Order created with ID ${order.id}, sales channel: ${order.salesChannel}`,
    );

    // Create order items
    if (items && items.length > 0) {
      const itemsToInsert = items.map((item: any) => ({
        orderId: order.id,
        productId: item.productId,
        quantity: item.quantity,
        unitPrice: item.unitPrice,
        total: item.total,
        discount: item.discount || "0.00", // Map discount here
        notes: item.notes || null,
      }));

      console.log(`Storage: Inserting ${itemsToInsert.length} order items`);
      const insertedItems = await database
        .insert(orderItemsTable)
        .values(itemsToInsert)
        .returning();

      console.log(`Storage: ${insertedItems.length} order items created`);

      // Update product stock for items that track inventory
      for (const item of items) {
        const [product] = await database
          .select()
          .from(products)
          .where(eq(products.id, item.productId))
          .limit(1);

        if (product && product.trackInventory) {
          const newStock = Math.max(0, product.stock - item.quantity);
          await database
            .update(products)
            .set({ stock: newStock })
            .where(eq(products.id, item.productId));

          console.log(
            `Storage: Updated stock for product ${item.productId}: ${product.stock} -> ${newStock}`,
          );
        }
      }
    }

    // Update table status if this is a table order
    if (orderData.tableId && orderData.salesChannel === "table") {
      await database
        .update(tables)
        .set({ status: "occupied" })
        .where(eq(tables.id, orderData.tableId));

      console.log(
        `Storage: Updated table ${orderData.tableId} status to occupied`,
      );
    }

    return order;
  }

  async updateOrder(
    id: number,
    orderData: Partial<{
      orderNumber: string;
      tableId: number | null;
      employeeId: number | null;
      status: string;
      customerName: string;
      customerCount: number;
      subtotal: string;
      tax: string;
      total: string;
      paymentMethod: string | null;
      paymentStatus: string;
      salesChannel: string;
      einvoiceStatus: number;
      templateNumber: string | null;
      symbol: string | null;
      invoiceNumber: string | null;
      invoiceId: number | null;
      notes: string | null;
      paidAt: Date | null;
      discount: string;
    }>,
    tenantDb?: any,
  ): Promise<Order | null> {
    const db = tenantDb || this.getSafeDatabase("updateOrder");
    console.log(
      `💾 Storage: Starting order update for ID ${id} with data:`,
      orderData,
    );

    // Fix timestamp handling - ensure Date objects
    if (orderData.paidAt && typeof orderData.paidAt === "string") {
      orderData.paidAt = new Date(orderData.paidAt);
    }

    // NO calculation - use exact frontend values
    console.log(
      `✅ Storage: Saving exact frontend values without any calculation or modification`,
    );

    const updateData = {
      ...orderData,
      updatedAt: new Date(),
    };

    console.log(`💾 Storage: Final update data for order ${id}:`, updateData);

    const [updatedOrder] = await db
      .update(orders)
      .set(updateData)
      .where(eq(orders.id, id))
      .returning();

    if (!updatedOrder) {
      console.error(`❌ Storage: No order returned after update for ID: ${id}`);
      return null;
    }

    console.log(`✅ Storage: Order ${id} updated successfully:`, {
      id: updatedOrder.id,
      orderNumber: updatedOrder.orderNumber,
      status: updatedOrder.status,
      subtotal: updatedOrder.subtotal,
      tax: updatedOrder.tax,
      discount: updatedOrder.discount,
      total: updatedOrder.total,
      paymentMethod: updatedOrder.paymentMethod,
      updatedAt: updatedOrder.updatedAt,
    });

    return updatedOrder;
  }

  // Validate database connection with comprehensive checks
  private validateDatabase(database: any, operation: string): void {
    if (!database) {
      console.error(`❌ Database is null/undefined in ${operation}`);
      throw new Error(`Database connection is not available for ${operation}`);
    }

    if (typeof database !== "object") {
      console.error(
        `❌ Database is not an object in ${operation}:`,
        typeof database,
      );
      throw new Error(`Invalid database type for ${operation}`);
    }

    if (!database.select || typeof database.select !== "function") {
      console.error(`❌ Database missing select method in ${operation}`);
      console.error(`❌ Available methods:`, Object.keys(database));
      throw new Error(
        `Database connection is invalid - missing select method for ${operation}`,
      );
    }

    if (!database.insert || typeof database.insert !== "function") {
      console.error(`) Database missing insert method in ${operation}`);
      throw new Error(
        `Database connection is invalid - missing insert method for ${operation}`,
      );
    }

    if (!database.update || typeof database.update !== "function") {
      console.error(`❌ Database missing update method in ${operation}`);
      throw new Error(
        `Database connection is invalid - missing update method for ${operation}`,
      );
    }
  }

  // Safe database query wrapper with enhanced error handling
  private async safeDbQuery<T>(
    queryFn: () => Promise<T>,
    fallbackValue: T,
    operation: string,
  ): Promise<T> {
    try {
      console.log(`🔍 Executing safe database query for ${operation}`);
      const result = await queryFn();
      console.log(
        `✅ Safe database query completed successfully for ${operation}`,
      );
      return result || fallbackValue;
    } catch (error) {
      console.error(`❌ Database error in ${operation}:`, {
        errorMessage: error?.message,
        errorType: error?.constructor?.name,
        errorStack: error?.stack,
      });

      // Check if it's a connection error specifically
      if (
        error?.message?.includes("select") ||
        error?.message?.includes("undefined")
      ) {
        console.error(
          `❌ CRITICAL: Database connection lost during ${operation}`,
        );
      }

      return fallbackValue;
    }
  }

  async updateOrderStatus(
    id: number | string,
    status: string,
    tenantDb?: any,
  ): Promise<Order | undefined> {
    console.log(`🚀 ========================================`);
    console.log(`🚀 STORAGE FUNCTION CALLED: updateOrderStatus`);
    console.log(`🚀 ========================================`);
    console.log(
      `📋 updateOrderStatus called with id: ${id}, status: ${status}`,
    );
    console.log(`🔍 updateOrderStatus parameters: {`);
    console.log(`  id: ${id},`);
    console.log(`  idType: '${typeof id}',`);
    console.log(`  status: '${status}',`);
    console.log(`  statusType: '${typeof status}',`);
    console.log(`  tenantDb: ${!!tenantDb}`);
    console.log(`}`);

    // Handle temporary order IDs - return a success response to continue flow
    if (typeof id === "string" && id.startsWith("temp-")) {
      console.log(
        `🟡 Temporary order ID detected: ${id} - allowing flow to continue to E-Invoice`,
      );

      // Return a success order object that allows the flow to continue to E-Invoice modal
      const mockOrder = {
        id: id as any, // Keep the temp ID for reference
        orderNumber: `TEMP-${new Date()}`,
        tableId: null,
        customerName: "Khách hàng",
        customerPhone: null,
        customerEmail: null,
        subtotal: "0.00",
        tax: "0.00",
        total: "0.00",
        status: status,
        paymentMethod: status === "paid" ? "cash" : null,
        paymentStatus: status === "paid" ? "paid" : "pending",
        einvoiceStatus: 0, // Not published yet
        invoiceNumber: null,
        templateNumber: null,
        symbol: null,
        notes: `Temporary order - payment flow continuing to E-Invoice`,
        orderedAt: new Date(),
        paidAt: status === "paid" ? new Date() : null,
        employeeId: null,
        salesChannel: "pos",
        createdAt: new Date(),
        updatedAt: new Date(),
      };

      console.log(
        `✅ Mock order created for temporary ID - flow will continue to E-Invoice:`,
        {
          id: mockOrder.id,
          status: mockOrder.status,
          paymentMethod: mockOrder.paymentMethod,
          allowsContinuation: true,
        },
      );

      return mockOrder;
    }

    // Enhanced database validation with comprehensive error handling
    let database;
    try {
      database = tenantDb || this.getSafeDatabase("updateOrderStatus");

      // Additional runtime validation
      if (!database || typeof database !== "object") {
        console.error(
          `❌ CRITICAL: Invalid database object in updateOrderStatus`,
        );
        throw new Error(`Database connection is completely invalid`);
      }

      if (!database.select || typeof database.select !== "function") {
        console.error(
          `❌ CRITICAL: Database missing select method in updateOrderStatus`,
        );
        console.error(`❌ Available methods:`, Object.keys(database));
        throw new Error(`Database connection is missing required methods`);
      }

      if (!database.update || typeof database.update !== "function") {
        console.error(
          `❌ CRITICAL: Database missing update method in updateOrderStatus`,
        );
        throw new Error(`Database connection is missing update method`);
      }

      console.log(`✅ Database validation passed for updateOrderStatus`);
    } catch (dbError) {
      console.error(
        `❌ Database validation failed in updateOrderStatus:`,
        dbError,
      );

      // Try to fall back to global db if tenant db is problematic
      if (
        tenantDb &&
        this.db &&
        typeof this.db === "object" &&
        this.db.select &&
        this.db.update
      ) {
        console.log(`🔄 Falling back to global database connection`);
        database = this.db;
      } else {
        console.error(`❌ No valid fallback database available`);
        throw new Error(
          `Database connection is completely unavailable: ${dbError.message}`,
        );
      }
    }

    // Ensure id is a number for database operations
    const orderId = typeof id === "string" ? parseInt(id) : id;
    if (isNaN(orderId)) {
      console.error(`❌ Invalid order ID: ${id}`);
      throw new Error(`Invalid order ID: ${id}`);
    }

    console.log(`🔍 Processing order ID: ${orderId} (type: ${typeof orderId})`);

    try {
      // First, get the current order to know its table
      console.log(`🔍 Fetching current order with ID: ${orderId}`);
      const result = await this.safeDbQuery(
        () =>
          database
            .select()
            .from(orders)
            .where(eq(orders.id, orderId as number)),
        [],
        `fetchCurrentOrder-${orderId}`,
      );
      const [currentOrder] = result;

      if (!currentOrder) {
        console.error(`❌ Order not found: ${orderId}`);
        console.log(`🔍 Attempting to fetch all orders to debug...`);
        try {
          const allOrders = await database.select().from(orders).limit(5);
          console.log(
            `🔍 Sample orders in database:`,
            allOrders.map((o) => ({
              id: o.id,
              orderNumber: o.orderNumber,
              status: o.status,
            })),
          );
        } catch (debugError) {
          console.error(`❌ Error fetching sample orders:`, debugError);
        }
        return undefined;
      }

      console.log(`📋 Current order before update:`, {
        id: currentOrder.id,
        orderNumber: currentOrder.orderNumber,
        tableId: currentOrder.tableId,
        currentStatus: currentOrder.status,
        requestedStatus: status,
        paidAt: currentOrder.paidAt,
        einvoiceStatus: currentOrder.einvoiceStatus,
      });

      // Update the order status with additional paid timestamp if needed
      const updateData: any = {
        status,
        updatedAt: new Date(),
      };

      if (status === "paid") {
        updateData.paidAt = new Date();
        console.log(
          `💳 Setting paidAt timestamp for order ${orderId}:`,
          updateData.paidAt,
        );
      }

      console.log(`🔍 Update data being sent:`, updateData);
      console.log(`🔍 Update query targeting order ID: ${orderId}`);

      const queryStartTime = new Date();
      console.log(`⏱️ DATABASE QUERY STARTED at:`, new Date());

      const updateResult = await this.safeDbQuery(
        () =>
          database
            .update(orders)
            .set(updateData)
            .where(eq(orders.id, orderId as number))
            .returning(),
        [],
        `updateOrderStatus-${orderId}`,
      );
      const [order] = updateResult;

      const queryEndTime = new Date();
      console.log(
        `⏱️ DATABASE QUERY COMPLETED in ${queryEndTime - queryStartTime}ms`,
      );
      console.log(`🔍 Database query execution result:`, {
        queryDuration: `${queryEndTime - queryStartTime}ms`,
        rowsAffected: order ? 1 : 0,
        orderReturned: !!order,
        timestamp: new Date(),
      });

      if (!order) {
        console.error(
          `❌ No order returned after status update for ID: ${orderId}`,
        );
        console.log(`🔍 Verifying order still exists...`);
        const [verifyOrder] = await database
          .select()
          .from(orders)
          .where(eq(orders.id, orderId as number));
        console.log(
          `🔍 Order verification result:`,
          verifyOrder ? "EXISTS" : "NOT FOUND",
        );
        return undefined;
      }

      console.log(`✅ Order status updated successfully:`, {
        id: order.id,
        orderNumber: order.orderNumber,
        tableId: order.tableId,
        previousStatus: currentOrder.status,
        newStatus: order.status,
        paidAt: order.paidAt,
        updatedAt: order.updatedAt,
        einvoiceStatus: order.einvoiceStatus,
      });

      // CRITICAL: Handle table status update when order is paid
      if (status === "paid" && order.tableId) {
        console.log(
          `💳 Order PAID - IMMEDIATELY processing table ${order.tableId} release`,
        );
        console.log(`🔍 DEBUG: Table release process started:`, {
          orderId: orderId,
          tableId: order.tableId,
          newStatus: status,
          timestamp: new Date(),
        });

        try {
          // Import tables from schema
          const { tables } = await import("@shared/schema");
          console.log(`✅ Tables schema imported successfully`);

          // Check for other ACTIVE orders on the same table (excluding current order and paid/cancelled orders)
          const activeStatuses = [
            "pending",
            "confirmed",
            "preparing",
            "ready",
            "served",
          ];
          console.log(
            `🔍 DEBUG: Checking for other active orders on table ${order.tableId}:`,
            {
              excludeOrderId: orderId,
              activeStatuses: activeStatuses,
              tableId: order.tableId,
            },
          );

          const otherActiveOrders = await database
            .select()
            .from(orders)
            .where(
              and(
                eq(orders.tableId, order.tableId),
                not(eq(orders.id, orderId as number)), // Exclude current order
                or(
                  ...activeStatuses.map((activeStatus) =>
                    eq(orders.status, activeStatus),
                  ),
                ),
              ),
            );

          console.log(
            `🔍 DEBUG: Query completed - found ${otherActiveOrders.length} other active orders`,
          );

          console.log(`🔍 Active orders remaining on table ${order.tableId}:`, {
            count: otherActiveOrders.length,
            orders: otherActiveOrders.map((o) => ({
              id: o.id,
              status: o.status,
              orderNumber: o.orderNumber,
            })),
          });

          // Get current table status
          const [currentTable] = await database
            .select()
            .from(tables)
            .where(eq(tables.id, order.tableId));

          if (!currentTable) {
            console.error(`❌ Table ${order.tableId} not found`);
          } else {
            console.log(`📋 Current table status:`, {
              id: currentTable.id,
              tableNumber: currentTable.tableNumber,
              status: currentTable.status,
            });

            // FORCE table release if no other active orders exist
            if (otherActiveOrders.length === 0) {
              console.log(
                `🔓 FORCING table ${order.tableId} release - no active orders remaining`,
              );
              console.log(`🔍 DEBUG: Table release attempt:`, {
                tableId: order.tableId,
                currentTableStatus: currentTable.status,
                targetStatus: "available",
                updateTimestamp: new Date(),
              });

              const [updatedTable] = await database
                .update(tables)
                .set({
                  status: "available",
                  updatedAt: new Date(),
                })
                .where(eq(tables.id, order.tableId))
                .returning();

              console.log(`🔍 DEBUG: Table update query result:`, {
                updatedTableExists: !!updatedTable,
                updatedTableData: updatedTable
                  ? {
                      id: updatedTable.id,
                      tableNumber: updatedTable.tableNumber,
                      status: updatedTable.status,
                      updatedAt: updatedTable.updatedAt,
                    }
                  : null,
              });

              if (updatedTable) {
                console.log(`✅ Table ${order.tableId} FORCEFULLY released:`, {
                  id: updatedTable.id,
                  tableNumber: updatedTable.tableNumber,
                  previousStatus: currentTable.status,
                  newStatus: updatedTable.status,
                  updateSuccess: true,
                });

                console.log(`🔍 DEBUG: Verifying table status after update...`);
                const [verifyTable] = await database
                  .select()
                  .from(tables)
                  .where(eq(tables.id, order.tableId));

                console.log(`🔍 DEBUG: Table verification result:`, {
                  tableFound: !!verifyTable,
                  verifiedStatus: verifyTable?.status,
                  verifiedUpdatedAt: verifyTable?.updatedAt,
                });
              } else {
                console.error(
                  `❌ CRITICAL: Failed to release table ${order.tableId} - no table returned`,
                );
                console.log(`🔍 DEBUG: Table update failed - investigating...`);

                // Debug: Check if table exists
                const [checkTable] = await database
                  .select()
                  .from(tables)
                  .where(eq(tables.id, order.tableId));

                console.log(`🔍 DEBUG: Table existence check:`, {
                  tableExists: !!checkTable,
                  tableData: checkTable
                    ? {
                        id: checkTable.id,
                        tableNumber: checkTable.tableNumber,
                        status: checkTable.status,
                      }
                    : null,
                });
              }
            } else {
              console.log(
                `🔒 Table ${order.tableId} remains occupied due to ${otherActiveOrders.length} active orders:`,
              );
              console.log(`🔍 DEBUG: Active orders preventing table release:`, {
                tableId: order.tableId,
                activeOrdersCount: otherActiveOrders.length,
                activeOrdersDetails: otherActiveOrders.map((o) => ({
                  id: o.id,
                  orderNumber: o.orderNumber,
                  status: o.status,
                })),
              });

              otherActiveOrders.forEach((activeOrder, index) => {
                console.log(
                  `   ${index + 1}. Order ${activeOrder.orderNumber} (${activeOrder.status}) - ID: ${activeOrder.id}`,
                );
              });
            }
          }
        } catch (tableError) {
          console.error(
            `❌ CRITICAL: Error processing table status update for table ${order.tableId}:`,
            tableError,
          );
          console.log(`🔍 DEBUG: Table update error details:`, {
            errorType: tableError?.constructor?.name,
            errorMessage: tableError?.message,
            errorStack: tableError?.stack,
            tableId: order.tableId,
            orderId: orderId,
          });
        }
      } else {
        console.log(
          `🔍 DEBUG: Order status is not 'paid' or no tableId - skipping table update:`,
          {
            orderStatus: status,
            tableId: order.tableId,
            isPaidStatus: status === "paid",
            hasTableId: !!order.tableId,
          },
        );
      }

      console.log(`🔍 DEBUG: Final order state before return:`, {
        orderId: order.id,
        orderNumber: order.orderNumber,
        status: order.status,
        tableId: order.tableId,
        paidAt: order.paidAt,
        updatedAt: order.updatedAt,
        updateSuccess: true,
      });
      return order;
    } catch (error) {
      console.error(`❌ Error updating order status:`, error);
      console.log(`🔍 DEBUG: Storage layer error details:`, {
        errorType: error?.constructor?.name,
        errorMessage: error?.message,
        errorStack: error?.stack,
        orderId: orderId,
        requestedStatus: status,
        timestamp: new Date(),
      });
      throw error;
    }
  }

  async addOrderItems(
    orderId: number,
    items: InsertOrderItem[],
    tenantDb?: any,
  ): Promise<OrderItem[]> {
    const database = tenantDb || this.getSafeDatabase("addOrderItems");
    const itemsWithOrderId = items.map((item) => ({ ...item, orderId }));
    return await database
      .insert(orderItemsTable)
      .values(itemsWithOrderId)
      .returning();
  }

  async removeOrderItem(itemId: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("removeOrderItem");
    const result = await database
      .delete(orderItemsTable)
      .where(eq(orderItemsTable.id, itemId));
    return (result.rowCount ?? 0) > 0;
  }

  async deleteOrderItem(itemId: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteOrderItem");
    const result = await database
      .delete(orderItemsTable)
      .where(eq(orderItemsTable.id, itemId));
    return (result.rowCount ?? 0) > 0;
  }

  async getOrderItems(orderId: number, tenantDb?: any): Promise<OrderItem[]> {
    const database = tenantDb || this.getSafeDatabase("getOrderItems");

    try {
      console.log(`🔍 Storage: Fetching order items for order ID ${orderId}`);

      if (!orderId || isNaN(orderId)) {
        console.error(`❌ Invalid order ID: ${orderId}`);
        return [];
      }

      const items = await database
        .select({
          id: orderItemsTable.id,
          orderId: orderItemsTable.orderId,
          productId: orderItemsTable.productId,
          quantity: orderItemsTable.quantity,
          unitPrice: orderItemsTable.unitPrice,
          total: orderItemsTable.total,
          discount: orderItemsTable.discount,
          notes: orderItemsTable.notes,
          productName: sql<string>`COALESCE(${products.name}, 'Unknown Product')`,
          productSku: sql<string>`COALESCE(${products.sku}, '')`,
        })
        .from(orderItemsTable)
        .leftJoin(products, eq(orderItemsTable.productId, products.id))
        .where(eq(orderItemsTable.orderId, orderId));

      console.log(
        `✅ Storage: Found ${items.length} order items for order ${orderId}`,
      );
      return Array.isArray(items) ? items : [];
    } catch (error) {
      console.error(
        `❌ Storage error fetching order items for order ${orderId}:`,
        error,
      );
      console.error("Error details:", {
        message: error?.message || "Unknown error",
        code: error?.code || "No code",
        stack: error?.stack || "No stack",
      });

      // Return empty array instead of throwing to prevent 500 errors
      return [];
    }
  }

  // Inventory Management
  async updateInventoryStock(
    productId: number,
    quantity: number,
    type: "add" | "subtract" | "set",
    notes?: string,
    tenantDb?: any,
  ): Promise<Product | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateInventoryStock");
    const product = await this.getProduct(productId, tenantDb);
    if (!product) return undefined;

    let newStock: number;

    switch (type) {
      case "add":
        newStock = product.stock + quantity;
        break;
      case "subtract":
        newStock = product.stock - quantity; // Use quantity directly, assuming it's already positive for subtraction
        break;
      case "set":
        newStock = quantity;
        break;
      default:
        return undefined;
    }

    // Ensure stock doesn't go below zero if tracking inventory
    if (product.trackInventory && newStock < 0) {
      console.warn(
        `Product ${product.name} stock would go below zero. Setting to 0.`,
      );
      newStock = 0;
    }

    const [updatedProduct] = await database
      .update(products)
      .set({ stock: newStock })
      .where(eq(products.id, productId))
      .returning();

    // Optionally, create an inventory transaction record here for audit purposes
    if (updatedProduct && notes) {
      await database.insert(inventoryTransactions).values({
        productId: productId,
        type: type,
        quantity: Math.abs(quantity), // Store absolute quantity
        unitPrice: product.price, // Or fetch from a price history if needed
        totalAmount: (product.price * Math.abs(quantity)).toFixed(2),
        referenceType: "manual_update", // Example reference type
        referenceId: null, // No specific reference ID for manual updates
        employeeId: null, // Could be passed as a parameter
        notes: notes,
        created_at: new Date(),
      });
    }

    return updatedProduct || undefined;
  }

  // Store Settings
  async getStoreSettings(tenantDb?: any): Promise<StoreSettings | null> {
    try {
      const database = tenantDb || this.getSafeDatabase("getStoreSettings");
      const [settings] = await database.select().from(storeSettings).limit(1);

      // If no settings exist, create default settings
      if (!settings) {
        const defaultSettings: InsertStoreSettings = {
          storeName: "EDPOS 레스토랑",
          storeCode: "STORE001",
          businessType: "restaurant",
          openTime: "09:00",
          closeTime: "22:00",
          currency: "VND", // Default currency
          taxRate: "8.00", // Default tax rate to 8%
          priceIncludesTax: false, // Default setting
          goldThreshold: "300000",
          vipThreshold: "1000000",
          defaultFloor: "1층", // Default floor
          defaultZone: "A", // Default zone
          floorPrefix: "층", // Floor prefix
          zonePrefix: "구역", // Zone prefix
        };
        const [newSettings] = await database
          .insert(storeSettings)
          .values(defaultSettings)
          .returning();
        return newSettings;
      }

      // Ensure default floor and zone are present, even if not in DB
      return {
        ...settings,
        defaultFloor: settings.defaultFloor || "1층",
        defaultZone: settings.defaultZone || "A",
        floorPrefix: settings.floorPrefix || "층",
        zonePrefix: settings.zonePrefix || "구역",
      };
    } catch (error) {
      console.error("❌ Error fetching store settings:", error);
      return null;
    }
  }

  async updateStoreSettings(
    data: Partial<InsertStoreSettings>,
    tenantDb?: any,
  ): Promise<StoreSettings> {
    const database = tenantDb || this.getSafeDatabase("updateStoreSettings");

    // Check if store settings exist
    const [existing] = await database.select().from(storeSettings).limit(1);

    if (existing) {
      // Update existing
      const [updated] = await database
        .update(storeSettings)
        .set({ ...data, updatedAt: new Date() })
        .where(eq(storeSettings.id, existing.id))
        .returning();
      return updated;
    } else {
      // Create new
      const [created] = await database
        .insert(storeSettings)
        .values({ ...data, createdAt: new Date(), updatedAt: new Date() })
        .returning();
      return created;
    }
  }

  // Suppliers
  async getSuppliers(tenantDb?: any): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getSuppliers");
    return await database.select().from(suppliers).orderBy(suppliers.name);
  }

  async getSupplier(id: number, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("getSupplier");
    const [result] = await database
      .select()
      .from(suppliers)
      .where(eq(suppliers.id, id));
    return result;
  }

  async getSuppliersByStatus(status: string, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("getSuppliersByStatus");
    return await database
      .select()
      .from(suppliers)
      .where(eq(suppliers.status, status))
      .orderBy(suppliers.name);
  }

  async searchSuppliers(query: string, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("searchSuppliers");
    return await database
      .select()
      .from(suppliers)
      .where(
        or(
          ilike(suppliers.name, `%${query}%`),
          ilike(suppliers.code, `%${query}%`),
          ilike(suppliers.contactPerson, `%${query}%`),
        ),
      )
      .orderBy(suppliers.name);
  }

  async createSupplier(data: InsertSupplier, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createSupplier");
    const [result] = await database.insert(suppliers).values(data).returning();
    return result;
  }

  async updateSupplier(
    id: number,
    data: Partial<InsertSupplier>,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateSupplier");
    const [result] = await database
      .update(suppliers)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(suppliers.id, id))
      .returning();

    return result;
  }

  async deleteSupplier(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteSupplier");
    const result = await database
      .delete(suppliers)
      .where(eq(suppliers.id, id))
      .returning();
    return result.length > 0;
  }

  // Customers
  async getCustomers(tenantDb?: any): Promise<Customer[]> {
    const database = tenantDb || this.getSafeDatabase("getCustomers");

    // Get membership thresholds
    const thresholds = await this.getMembershipThresholds(tenantDb);

    // Get all customers
    const allCustomers = await database
      .select()
      .from(customers)
      .orderBy(customers.name);

    // Update membership levels based on spending
    const updatedCustomers = [];
    for (const customer of allCustomers) {
      const totalSpent = parseFloat(customer.totalSpent || "0");
      const calculatedLevel = this.calculateMembershipLevel(
        totalSpent,
        thresholds.GOLD,
        thresholds.VIP,
      );

      // Update if membership level has changed
      if (customer.membershipLevel !== calculatedLevel) {
        const [updatedCustomer] = await database
          .update(customers)
          .set({
            membershipLevel: calculatedLevel,
            updatedAt: new Date(),
          })
          .where(eq(customers.id, customer.id))
          .returning();
        updatedCustomers.push(updatedCustomer);
      } else {
        updatedCustomers.push(customer);
      }
    }

    return updatedCustomers;
  }

  async searchCustomers(query: string, tenantDb?: any): Promise<Customer[]> {
    const database = tenantDb || this.getSafeDatabase("searchCustomers");
    return await database
      .select()
      .from(customers)
      .where(
        or(
          ilike(customers.name, `%${query}%`),
          ilike(customers.customerId, `%${query}%`),
          ilike(customers.phone, `%${query}%`),
          ilike(customers.email, `%${query}%`),
        ),
      )
      .orderBy(customers.name);
  }

  async getCustomer(id: number, tenantDb?: any): Promise<Customer | undefined> {
    const database = tenantDb || this.getSafeDatabase("getCustomer");
    const [result] = await database
      .select()
      .from(customers)
      .where(eq(customers.id, id));
    return result || undefined;
  }

  async getCustomerByCustomerId(
    customerId: string,
    tenantDb?: any,
  ): Promise<Customer | undefined> {
    const database =
      tenantDb || this.getSafeDatabase("getCustomerByCustomerId");
    const [result] = await database
      .select()
      .from(customers)
      .where(eq(customers.customerId, customerId));
    return result || undefined;
  }

  async createCustomer(
    customerData: InsertCustomer,
    tenantDb?: any,
  ): Promise<Customer> {
    const database = tenantDb || this.getSafeDatabase("createCustomer");
    // Generate customer ID if not provided
    if (!customerData.customerId) {
      const count = await database
        .select({ count: sql<number>`count(*)` })
        .from(customers);
      const customerCount = count[0]?.count || 0;
      customerData.customerId = `CUST${String(customerCount + 1).padStart(3, "0")}`;
    }

    const [result] = await database
      .insert(customers)
      .values(customerData)
      .returning();
    return result;
  }

  async updateCustomer(
    id: number,
    customerData: Partial<InsertCustomer>,
    tenantDb?: any,
  ): Promise<Customer | undefined> {
    const database = tenantDb || this.getSafeDatabase("updateCustomer");
    const [result] = await database
      .update(customers)
      .set({ ...customerData, updatedAt: new Date() })
      .where(eq(customers.id, id))
      .returning();
    return result || undefined;
  }

  async deleteCustomer(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteCustomer");
    const result = await database
      .delete(customers)
      .where(eq(customers.id, id))
      .returning();
    return result.length > 0;
  }

  async updateCustomerVisit(
    customerId: number,
    amount: number,
    points: number,
    tenantDb?: any,
  ) {
    const database = tenantDb || this.getSafeDatabase("updateCustomerVisit");
    const [customer] = await database
      .select()
      .from(customers)
      .where(eq(customers.id, customerId));

    if (!customer) {
      throw new Error("Customer not found");
    }

    const newTotalSpent = parseFloat(customer.totalSpent || "0") + amount;
    const newVisitCount = (customer.visitCount || 0) + 1;
    const newPoints = (customer.points || 0) + points;

    // Get membership thresholds and calculate new level
    const thresholds = await this.getMembershipThresholds(tenantDb);
    const newMembershipLevel = this.calculateMembershipLevel(
      newTotalSpent,
      thresholds.GOLD,
      thresholds.VIP,
    );

    const [updated] = await database
      .update(customers)
      .set({
        visitCount: newVisitCount,
        totalSpent: newTotalSpent.toString(),
        points: newPoints,
        membershipLevel: newMembershipLevel,
        updatedAt: new Date(),
      })
      .where(eq(customers.id, customerId))
      .returning();

    return updated;
  }

  // Point Management Methods
  async getCustomerPoints(
    customerId: number,
    tenantDb?: any,
  ): Promise<{ points: number } | undefined> {
    const database = tenantDb || this.getSafeDatabase("getCustomerPoints");
    const customer = await this.getCustomer(customerId, tenantDb);
    if (!customer) return undefined;
    return { points: customer.points || 0 };
  }

  async updateCustomerPoints(
    customerId: number,
    points: number,
    description: string,
    type: "earned" | "redeemed" | "adjusted",
    employeeId?: number,
    orderId?: number,
    tenantDb?: any,
  ): Promise<PointTransaction> {
    const database = tenantDb || this.getSafeDatabase("updateCustomerPoints");
    const customer = await this.getCustomer(customerId, tenantDb);
    if (!customer) throw new Error("Customer not found");

    const previousBalance = customer.points || 0;
    let pointChange = points;

    // For redeemed points, make sure it's negative
    if (type === "redeemed" && pointChange > 0) {
      pointChange = -pointChange;
    }

    const newBalance = previousBalance + pointChange;

    // Ensure customer doesn't go below 0 points for redemption
    if (newBalance < 0) {
      throw new Error("Insufficient points balance");
    }

    // Update customer points
    await database
      .update(customers)
      .set({
        points: newBalance,
        updatedAt: new Date(),
      })
      .where(eq(customers.id, customerId));

    // Create point transaction record
    const [pointTransaction] = await database
      .insert(pointTransactions)
      .values({
        customerId,
        type,
        points: pointChange,
        description,
        orderId,
        employeeId,
        previousBalance,
        newBalance,
      })
      .returning();

    return pointTransaction;
  }

  async getPointHistory(
    customerId: number,
    limit: number = 50,
    tenantDb?: any,
  ): Promise<PointTransaction[]> {
    const database = tenantDb || this.getSafeDatabase("getPointHistory");
    return await database
      .select()
      .from(pointTransactions)
      .where(eq(pointTransactions.customerId, customerId))
      .orderBy(sql`${pointTransactions.createdAt} DESC`)
      .limit(limit);
  }

  async getAllPointTransactions(
    limit: number = 100,
    tenantDb?: any,
  ): Promise<PointTransaction[]> {
    const database =
      tenantDb || this.getSafeDatabase("getAllPointTransactions");
    return await database
      .select()
      .from(pointTransactions)
      .orderBy(sql`${pointTransactions.createdAt} DESC`)
      .limit(limit);
  }

  // Get membership thresholds
  async getMembershipThresholds(
    tenantDb?: any,
  ): Promise<{ GOLD: number; VIP: number }> {
    const database =
      tenantDb || this.getSafeDatabase("getMembershipThresholds");
    try {
      const [settings] = await database.select().from(storeSettings).limit(1);

      if (!settings) {
        // Return default values if no settings exist
        return { GOLD: 300000, VIP: 1000000 };
      }

      // Parse thresholds from settings or return defaults
      const goldThreshold =
        parseInt(settings.goldThreshold as string) || 300000;
      const vipThreshold = parseInt(settings.vipThreshold as string) || 1000000;

      return { GOLD: goldThreshold, VIP: vipThreshold };
    } catch (error) {
      console.error("Error fetching membership thresholds:", error);
      return { GOLD: 300000, VIP: 1000000 };
    }
  }

  // Calculate membership level based on total spent
  private calculateMembershipLevel(
    totalSpent: number,
    goldThreshold: number,
    vipThreshold: number,
  ): string {
    if (totalSpent >= vipThreshold) return "VIP";
    if (totalSpent >= goldThreshold) return "GOLD";
    return "SILVER";
  }

  async updateMembershipThresholds(
    thresholds: {
      GOLD: number;
      VIP: number;
    },
    tenantDb?: any,
  ): Promise<{ GOLD: number; VIP: number }> {
    const database =
      tenantDb || this.getSafeDatabase("updateMembershipThresholds");
    try {
      // Update or insert store settings with thresholds
      const currentSettings = await this.getStoreSettings(tenantDb);

      await database
        .update(storeSettings)
        .set({
          goldThreshold: thresholds.GOLD.toString(),
          vipThreshold: thresholds.VIP.toString(),
          updatedAt: new Date(),
        })
        .where(eq(storeSettings.id, currentSettings.id));

      // Recalculate all customer membership levels with new thresholds
      await this.recalculateAllMembershipLevels(
        thresholds.GOLD,
        thresholds.VIP,
        tenantDb,
      );

      return thresholds;
    } catch (error) {
      console.error("Error updating membership thresholds:", error);
      throw error;
    }
  }

  // Recalculate membership levels for all customers
  async recalculateAllMembershipLevels(
    goldThreshold: number,
    vipThreshold: number,
    tenantDb?: any,
  ) {
    const database =
      tenantDb || this.getSafeDatabase("recalculateAllMembershipLevels");
    const allCustomers = await database.select().from(customers);

    for (const customer of allCustomers) {
      const totalSpent = parseFloat(customer.totalSpent || "0");
      const calculatedLevel = this.calculateMembershipLevel(
        totalSpent,
        goldThreshold,
        vipThreshold,
      );

      if (customer.membershipLevel !== calculatedLevel) {
        await database
          .update(customers)
          .set({
            membershipLevel: calculatedLevel,
            updatedAt: new Date(),
          })
          .where(eq(customer.id, customer.id));
      }
    }
  }

  async getAllProducts(
    includeInactive: boolean = false,
    tenantDb?: any,
  ): Promise<Product[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getAllProducts");
      let result;
      if (includeInactive) {
        result = await database.select().from(products).orderBy(products.name);
      } else {
        result = await database
          .select()
          .from(products)
          .where(eq(products.isActive, true))
          .orderBy(products.name);
      }

      // Ensure afterTaxPrice and beforeTaxPrice are properly returned
      return result.map((product) => ({
        ...product,
        afterTaxPrice: product.afterTaxPrice || null,
        beforeTaxPrice: product.beforeTaxPrice || null,
        floor: product.floor || "1층", // Default floor
      }));
    } catch (error) {
      console.error(`❌ Error in getAllProducts:`, error);
      return [];
    }
  }

  async getActiveProducts(tenantDb?: any): Promise<Product[]> {
    const database = tenantDb || this.getSafeDatabase("getActiveProducts");
    const result = await database
      .select()
      .from(products)
      .where(eq(products.isActive, true))
      .orderBy(products.name);

    // Ensure afterTaxPrice and beforeTaxPrice are properly returned
    return result.map((product) => ({
      ...product,
      afterTaxPrice: product.afterTaxPrice || null,
      beforeTaxPrice: product.beforeTaxPrice || null,
      floor: product.floor || "1층", // Default floor
    }));
  }

  // Note: Removed duplicate createProduct method to avoid conflicts
  // All product creation now uses the createProduct method with tenantDb parameter (line 872)

  // Invoice template methods
  async getInvoiceTemplates(tenantDb: any = null): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getInvoiceTemplates");
    try {
      const templates = await database
        .select()
        .from(invoiceTemplates)
        .orderBy(desc(invoiceTemplates.id));
      return templates;
    } catch (error) {
      console.error("Error fetching invoice templates:", error);
      return [];
    }
  }

  async getActiveInvoiceTemplates(tenantDb: any = null): Promise<any[]> {
    const database =
      tenantDb || this.getSafeDatabase("getActiveInvoiceTemplates");
    try {
      const templates = await database
        .select()
        .from(invoiceTemplates)
        .where(eq(invoiceTemplates.isDefault, true))
        .orderBy(desc(invoiceTemplates.id));
      return templates;
    } catch (error) {
      console.error("Error fetching active invoice templates:", error);
      return [];
    }
  }

  async createInvoiceTemplate(
    templateData: any,
    tenantDb: any = null,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createInvoiceTemplate");
    try {
      const [template] = await database
        .insert(invoiceTemplates)
        .values({
          name: templateData.name,
          templateNumber: templateData.templateNumber,
          templateCode: templateData.templateCode || null,
          symbol: templateData.symbol,
          useCK: templateData.useCK !== false,
          notes: templateData.notes || null,
          isDefault: templateData.isDefault || false,
          createdAt: new Date(),
        })
        .returning();
      return template;
    } catch (error) {
      console.error("Error creating invoice template:", error);
      throw error;
    }
  }

  async updateInvoiceTemplate(
    id: number,
    templateData: any,
    tenantDb: any = null,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateInvoiceTemplate");
    try {
      const [template] = await database
        .update(invoiceTemplates)
        .set({
          name: templateData.name,
          templateNumber: templateData.templateNumber,
          templateCode: templateData.templateCode || null,
          symbol: templateData.symbol,
          useCK: templateData.useCK !== false,
          notes: templateData.notes || null,
          isDefault: templateData.isDefault || false,
          updatedAt: new Date(),
        })
        .where(eq(invoiceTemplates.id, id))
        .returning();
      return template;
    } catch (error) {
      console.error("Error updating invoice template:", error);
      throw error;
    }
  }

  async deleteInvoiceTemplate(
    id: number,
    tenantDb: any = null,
  ): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteInvoiceTemplate");
    try {
      const [deleted] = await database
        .delete(invoiceTemplates)
        .where(eq(invoiceTemplates.id, id))
        .returning();
      return !!deleted;
    } catch (error) {
      console.error("Error deleting invoice template:", error);
      throw error;
    }
  }

  // Invoice methods
  async getInvoices(tenantDb?: any): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getInvoices");
    return await database
      .select()
      .from(invoices)
      .orderBy(desc(invoices.createdAt));
  }

  async getInvoice(id: number, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("getInvoice");
    const [invoice] = await database
      .select()
      .from(invoices)
      .where(eq(invoices.id, id));

    if (!invoice) return null;

    // Get invoice items
    const items = await database
      .select()
      .from(invoiceItems)
      .where(eq(invoiceItems.invoiceId, id));

    return {
      ...invoice,
      items: items,
    };
  }

  async createInvoice(invoiceData: any, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createInvoice");

    console.log("💾 Creating invoice in database:", invoiceData);

    try {
      // Handle date conversion properly
      let invoiceDate = new Date();
      if (invoiceData.invoiceDate) {
        if (invoiceData.invoiceDate instanceof Date) {
          invoiceDate = invoiceData.invoiceDate;
        } else if (typeof invoiceData.invoiceDate === "string") {
          invoiceDate = new Date(invoiceData.invoiceDate);
        }
      }

      // Insert invoice
      const [invoice] = await database
        .insert(invoices)
        .values({
          invoiceNumber: invoiceData.invoiceNumber || null,
          templateNumber: invoiceData.templateNumber || null,
          symbol: invoiceData.symbol || null,
          customerName: invoiceData.customerName,
          customerTaxCode: invoiceData.customerTaxCode || null,
          customerAddress: invoiceData.customerAddress || null,
          customerPhone: invoiceData.customerPhone || null,
          customerEmail: invoiceData.customerEmail || null,
          subtotal: invoiceData.subtotal,
          tax: invoiceData.tax,
          total: invoiceData.total,
          paymentMethod: invoiceData.paymentMethod || 1,
          invoiceDate: invoiceDate,
          status: invoiceData.status || "draft",
          einvoiceStatus: invoiceData.einvoiceStatus || 0,
          notes: invoiceData.notes || null,
        })
        .returning();

      console.log("✅ Invoice created:", invoice);

      // Insert invoice items if provided
      if (
        invoiceData.items &&
        Array.isArray(invoiceData.items) &&
        invoiceData.items.length > 0
      ) {
        const itemsToInsert = invoiceData.items.map((item: any) => ({
          invoiceId: invoice.id,
          productId: item.productId,
          productName: item.productName,
          quantity: item.quantity,
          unitPrice: item.unitPrice,
          total: item.total,
          taxRate: item.taxRate || "0.00",
        }));

        await database.insert(invoiceItems).values(itemsToInsert);
        console.log(`✅ Inserted ${itemsToInsert.length} invoice items`);
      }

      return invoice;
    } catch (error) {
      console.error("❌ Error creating invoice:", error);
      throw error;
    }
  }

  async updateInvoice(
    id: number,
    updateData: any,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateInvoice");

    const [invoice] = await database
      .update(invoices)
      .set({
        ...updateData,
        updatedAt: new Date(),
      })
      .where(eq(invoices.id, id))
      .returning();

    return invoice;
  }

  async deleteInvoice(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deleteInvoice");

    // Delete invoice items first
    await database.delete(invoiceItems).where(eq(invoiceItems.invoiceId, id));

    // Delete invoice
    const result = await database.delete(invoices).where(eq(invoices.id, id));

    return result.rowCount > 0;
  }

  // E-invoice connections methods
  async getEInvoiceConnections(tenantDb?: any): Promise<any[]> {
    const database = tenantDb || this.getSafeDatabase("getEInvoiceConnections");
    try {
      const { eInvoiceConnections } = await import("@shared/schema");
      return await database
        .select()
        .from(eInvoiceConnections)
        .orderBy(eInvoiceConnections.symbol);
    } catch (error) {
      console.error("Error fetching e-invoice connections:", error);
      return [];
    }
  }

  async getEInvoiceConnection(id: number, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("getEInvoiceConnection");
    try {
      const { eInvoiceConnections } = await import("@shared/schema");
      const [result] = await database
        .select()
        .from(eInvoiceConnections)
        .where(eq(eInvoiceConnections.id, id));
      return result;
    } catch (error) {
      console.error("Error fetching e-invoice connection:", error);
      return null;
    }
  }

  async createEInvoiceConnection(data: any, tenantDb?: any): Promise<any> {
    const database =
      tenantDb || this.getSafeDatabase("createEInvoiceConnection");
    try {
      const { eInvoiceConnections } = await import("@shared/schema");

      // Generate next symbol number
      const existingConnections = await this.getEInvoiceConnections(tenantDb);
      const nextSymbol = (existingConnections.length + 1).toString();

      const [result] = await database
        .insert(eInvoiceConnections)
        .values({
          ...data,
          symbol: nextSymbol,
          createdAt: new Date(),
          updatedAt: new Date(),
        })
        .returning();
      return result;
    } catch (error) {
      console.error("Error creating e-invoice connection:", error);
      throw error;
    }
  }

  async updateEInvoiceConnection(
    id: number,
    data: any,
    tenantDb?: any,
  ): Promise<any> {
    const database =
      tenantDb || this.getSafeDatabase("updateEInvoiceConnection");
    try {
      const { eInvoiceConnections } = await import("@shared/schema");
      const [result] = await database
        .update(eInvoiceConnections)
        .set({ ...data, updatedAt: new Date() })
        .where(eq(eInvoiceConnections.id, id))
        .returning();
      return result;
    } catch (error) {
      console.error("Error updating e-invoice connection:", error);
      throw error;
    }
  }

  async deleteEInvoiceConnection(id: number, tenantDb?: any): Promise<boolean> {
    const database =
      tenantDb || this.getSafeDatabase("deleteEInvoiceConnection");
    try {
      const { eInvoiceConnections } = await import("@shared/schema");
      const result = await database
        .delete(eInvoiceConnections)
        .where(eq(eInvoiceConnections.id, id))
        .returning();
      return result.length > 0;
    } catch (error) {
      console.error("Error deleting e-invoice connection:", error);
      return false;
    }
  }

  async getEmployeeByEmail(
    email: string,
    tenantDb?: any,
  ): Promise<Employee | undefined> {
    const database = tenantDb || this.getSafeDatabase("getEmployeeByEmail");
    if (email && email.trim() !== "") {
      const [employee] = await database
        .select()
        .from(employees)
        .where(eq(employees.email, email));

      return employee || undefined;
    }
    return undefined;
  }

  // New function to get attendance records by date range
  async getAttendanceRecordsByRange(
    startDate: string,
    endDate: string,
    tenantDb?: any,
  ): Promise<AttendanceRecord[]> {
    const database =
      tenantDb || this.getSafeDatabase("getAttendanceRecordsByRange");

    try {
      console.log(
        `🔍 Getting attendance records for date range: ${startDate} to ${endDate}`,
      );

      // Ensure dates are valid and set to start/end of day
      const startOfRange = new Date(startDate);
      if (isNaN(startOfRange.getTime())) {
        throw new Error(`Invalid start date provided: ${startDate}`);
      }
      startOfRange.setHours(0, 0, 0, 0);

      const endOfRange = new Date(endDate);
      if (isNaN(endOfRange.getTime())) {
        throw new Error(`Invalid end date provided: ${endDate}`);
      }
      endOfRange.setHours(23, 59, 59, 999);

      console.log(`🔍 Date range for query: ${startOfRange} to ${endOfRange}`);

      const records = await database
        .select()
        .from(attendanceRecords)
        .where(
          and(
            gte(attendanceRecords.clockIn, startOfRange),
            lte(attendanceRecords.clockIn, endOfRange),
          ),
        )
        .orderBy(attendanceRecords.clockIn);

      console.log(
        `✅ Found ${records.length} attendance records in date range`,
      );
      return records;
    } catch (error) {
      console.error("Error fetching attendance records by range:", error);
      // Re-throw the error to be handled by the caller
      throw error;
    }
  }

  // Printer configuration management
  async getPrinterConfigs(tenantDb?: any): Promise<PrinterConfig[]> {
    const database = tenantDb || this.getSafeDatabase("getPrinterConfigs");
    console.log(
      "🔍 Storage: Fetching all printer configs (active and inactive)",
    );

    try {
      const configs = await database
        .select()
        .from(printerConfigs)
        .orderBy(printerConfigs.id);
      console.log(`✅ Storage: Found ${configs.length} printer configs`);
      return configs;
    } catch (error) {
      console.error("❌ Storage: Error fetching printer configs:", error);
      return [];
    }
  }

  async createPrinterConfig(
    configData: any,
    tenantDb?: any,
  ): Promise<PrinterConfig> {
    const database = tenantDb || this.getSafeDatabase("createPrinterConfig");
    const [config] = await database
      .insert(printerConfigs)
      .values(configData)
      .returning();
    return config;
  }

  async updatePrinterConfig(
    id: number,
    configData: any,
    tenantDb?: any,
  ): Promise<PrinterConfig | null> {
    const database = tenantDb || this.getSafeDatabase("updatePrinterConfig");
    const [config] = await database
      .update(printerConfigs)
      .set({ ...configData, updatedAt: new Date() })
      .where(eq(printerConfigs.id, id))
      .returning();
    return config || null;
  }

  async deletePrinterConfig(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deletePrinterConfig");
    const result = await database
      .delete(printerConfigs)
      .where(eq(printerConfigs.id, id));
    return result.rowCount > 0;
  }

  // Purchase Order Management Implementation
  async getPurchaseOrders(tenantDb?: any): Promise<PurchaseReceipt[]> {
    const database = tenantDb || this.getSafeDatabase("getPurchaseOrders");
    try {
      console.log(
        "🔍 Starting getPurchaseOrders - using purchase_receipts table",
      );

      // Query purchase_receipts table directly
      const result = await database.execute(sql`
        SELECT 
          pr.*,
          s.name as supplierName
        FROM purchase_receipts pr
        LEFT JOIN suppliers s ON pr.supplier_id = s.id
        ORDER BY pr.created_at DESC
      `);

      console.log(
        `✅ Query executed successfully, found ${result.rows?.length || 0} purchase receipts`,
      );
      return result.rows || [];
    } catch (error) {
      console.error(`❌ Error in getPurchaseOrders:`, error);
      return [];
    }
  }

  async getPurchaseOrder(
    id: number,
    tenantDb?: any,
  ): Promise<PurchaseReceipt | null> {
    const database = tenantDb || this.getSafeDatabase("getPurchaseOrder");
    console.log(`🔍 Starting getPurchaseOrder for ID: ${id}`);

    try {
      // Simple query without complex joins to avoid SQL syntax errors
      const [receipt] = await database
        .select()
        .from(purchaseReceipts)
        .where(eq(purchaseReceipts.id, id))
        .limit(1);

      if (!receipt) {
        console.log(`❌ Purchase receipt not found: ${id}`);
        return null;
      }

      console.log(`✅ Purchase receipt found: ${receipt.id}`);
      return receipt;
    } catch (error) {
      console.error(`❌ Error in getPurchaseOrder for ID ${id}:`, error);
      throw error;
    }
  }

  async getPurchaseOrdersBySupplier(
    supplierId: number,
    tenantDb?: any,
  ): Promise<PurchaseReceipt[]> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("getPurchaseOrdersBySupplier");
      const result = await database.query(
        `
        SELECT 
          pr.*,
          s.name as supplierName
        FROM purchase_receipts pr
        LEFT JOIN suppliers s ON pr.supplier_id = s.id
        WHERE pr.supplier_id = $1
        ORDER BY pr.created_at DESC
      `,
        [supplierId],
      );
      return result.rows || [];
    } catch (error) {
      console.error(`❌ Error in getPurchaseOrdersBySupplier:`, error);
      return [];
    }
  }

  async getPurchaseOrdersByStatus(
    status: string,
    tenantDb?: any,
  ): Promise<PurchaseReceipt[]> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("getPurchaseOrdersByStatus");
      const result = await database.query(
        `
        SELECT 
          pr.*,
          s.name as supplierName
        FROM purchase_receipts pr
        LEFT JOIN suppliers s ON pr.supplier_id = s.id
        WHERE pr.status = $1
        ORDER BY pr.created_at DESC
      `,
        [status],
      );
      return result.rows || [];
    } catch (error) {
      console.error(`❌ Error in getPurchaseOrdersByStatus:`, error);
      return [];
    }
  }

  async searchPurchaseOrders(
    query: string,
    tenantDb?: any,
  ): Promise<PurchaseReceipt[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("searchPurchaseOrders");
      const result = await database.query(
        `
        SELECT 
          pr.*,
          s.name as supplierName
        FROM purchase_receipts pr
        LEFT JOIN suppliers s ON pr.supplier_id = s.id
        WHERE pr.po_number ILIKE $1 OR s.name ILIKE $1 OR pr.notes ILIKE $1
        ORDER BY pr.created_at DESC
      `,
        [`%${query}%`],
      );
      return result.rows || [];
    } catch (error) {
      console.error(`❌ Error in searchPurchaseOrders:`, error);
      return [];
    }
  }

  async createPurchaseOrder(
    orderData: InsertPurchaseReceipt,
    items: InsertPurchaseReceiptItem[],
    tenantDb?: any,
  ): Promise<PurchaseReceipt> {
    const database = tenantDb || this.getSafeDatabase("createPurchaseOrder");
    try {
      console.log("🔍 Creating purchase order with data:", orderData);
      console.log("🔍 Creating purchase order with items:", items);

      // Insert into purchase_receipts table
      const insertResult = await database.execute(sql`
        INSERT INTO purchase_receipts (
          receipt_number, supplier_id, employee_id, status, purchase_date,
          subtotal, tax, total, notes, created_at, updated_at
        ) VALUES (
          ${orderData.poNumber}, ${orderData.supplierId}, ${orderData.employeeId || null}, 
          ${orderData.status || "pending"}, ${orderData.purchaseDate || null},
          ${orderData.subtotal}, ${orderData.tax}, ${orderData.total}, 
          ${orderData.notes || null}, NOW(), NOW()
        ) RETURNING *
      `);

      if (!insertResult.rows || insertResult.rows.length === 0) {
        throw new Error("Failed to create purchase order");
      }

      const newOrder = insertResult.rows[0];
      console.log("✅ Purchase order created:", newOrder);

      // Insert items into purchase_receipt_items table
      if (items && items.length > 0) {
        for (const item of items) {
          await database.execute(sql`
            INSERT INTO purchase_receipt_items (
              purchase_receipt_id, product_id, product_name, sku, quantity, received_quantity, unit_price, total
            ) VALUES (
              ${newOrder.id}, ${item.productId}, ${item.productName}, ${item.sku || ""},
              ${item.quantity}, ${item.receivedQuantity || 0}, ${item.unitPrice}, ${item.total}
            )
          `);
        }
        console.log(`✅ Created ${items.length} purchase receipt items`);
      }

      return {
        id: newOrder.id,
        receiptNumber: newOrder.receipt_number,
        supplierId: newOrder.supplier_id,
        employeeId: newOrder.employee_id,
        status: newOrder.status,
        purchaseDate: newOrder.purchase_date,
        actualDeliveryDate: newOrder.actual_delivery_date,
        subtotal: newOrder.subtotal,
        tax: newOrder.tax,
        total: newOrder.total,
        notes: newOrder.notes,
        createdAt: newOrder.created_at,
        updatedAt: newOrder.updated_at,
      };
    } catch (error) {
      console.error("❌ Error creating purchase order:", error);
      throw error;
    }
  }

  async updatePurchaseOrder(
    id: number,
    updateData: Partial<InsertPurchaseReceipt>,
    tenantDb?: any,
  ): Promise<PurchaseReceipt | null> {
    try {
      const database = tenantDb || this.getSafeDatabase("updatePurchaseOrder");

      // Start transaction
      await database.query("BEGIN");

      // Update main purchase receipt
      await database.query(
        `
        UPDATE purchase_receipts SET 
          supplier_id = $1, 
          po_number = $2, 
          purchase_date = $3, 
          status = $4, 
          subtotal = $5, 
          tax = $6, 
          discount = $7, 
          total = $8, 
          notes = $9, 
          purchase_type = $10,
          updated_at = CURRENT_TIMESTAMP
        WHERE id = $11
      `,
        [
          updateData.supplierId,
          updateData.poNumber,
          updateData.purchaseDate,
          updateData.status || "pending",
          updateData.subtotal,
          updateData.tax,
          updateData.discount || "0.00",
          updateData.total,
          updateData.notes,
          updateData.purchaseType,
          id,
        ],
      );

      // Delete existing items
      await database.query(
        "DELETE FROM purchase_receipt_items WHERE purchase_receipt_id = $1",
        [id],
      );

      // Insert updated items
      if (updateData.items && updateData.items.length > 0) {
        for (const item of updateData.items) {
          await database.query(
            `
            INSERT INTO purchase_receipt_items (
              purchase_receipt_id, product_id, product_name, sku, quantity, received_quantity, unit_price, total
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          `,
            [
              id,
              item.productId,
              item.productName,
              item.sku,
              item.quantity,
              item.receivedQuantity || 0,
              item.unitPrice,
              item.total,
            ],
          );
        }
      }

      await database.query("COMMIT");

      return { id };
    } catch (error) {
      console.error("❌ Error in updatePurchaseOrder:", error);
      await database.query("ROLLBACK");
      throw error;
    }
  }

  async deletePurchaseOrder(id: number, tenantDb?: any): Promise<boolean> {
    const database = tenantDb || this.getSafeDatabase("deletePurchaseOrder");
    try {
      // First, delete all related items from purchase_receipt_items
      const itemDeleteResult = await database.execute(sql`
        DELETE FROM purchase_receipt_items 
        WHERE purchase_receipt_id = ${id}
      `);

      console.log(`🗑️ Deleted purchase receipt items for order ${id}`);

      // Then delete the purchase receipt using sql helper
      const orderDeleteResult = await database.execute(sql`
        DELETE FROM purchase_receipts 
        WHERE id = ${id}
      `);

      console.log(`🗑️ Deleted purchase receipt with ID ${id}`);

      return true;
    } catch (error) {
      console.error(`❌ Error in deletePurchaseOrder:`, error);
      throw error;
    }
  }

  async updatePurchaseOrderStatus(
    id: number,
    status: string,
    tenantDb?: any,
  ): Promise<PurchaseReceipt | null> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("updatePurchaseOrderStatus");
      const [updatedOrder] = await database
        .update(purchaseReceipts)
        .set({
          status,
          updatedAt: new Date(),
        })
        .where(eq(purchaseReceipts.id, id))
        .returning();
      return updatedOrder || null;
    } catch (error) {
      console.error(`❌ Error in updatePurchaseOrderStatus:`, error);
      throw error;
    }
  }

  async getNextPONumber(tenantDb?: any): Promise<string> {
    try {
      const database = tenantDb || this.getSafeDatabase("getNextPONumber");
      console.log("🔍 Generating next purchase receipt number...");

      // Get current year (last 2 digits)
      const currentYear = new Date().getFullYear().toString().slice(-2);

      // Query for the last PO number of current year using LIKE pattern
      const lastPOQuery = await database.execute(sql`
        SELECT receipt_number 
        FROM purchase_receipts 
        WHERE receipt_number LIKE ${'PN%/' + currentYear}
        ORDER BY receipt_number DESC 
        LIMIT 1
      `);

      console.log("📋 Last PO number found for year", currentYear, ":", lastPOQuery.rows[0]?.receipt_number);

      let nextSequence = 1;

      if (lastPOQuery.rows && lastPOQuery.rows.length > 0) {
        const lastPONumber = lastPOQuery.rows[0].receipt_number;
        // Extract sequence number from format: PNxxxxxx/YY
        const match = lastPONumber.match(/^PN(\d{6})\/\d{2}$/);
        if (match) {
          const lastSequence = parseInt(match[1], 10);
          nextSequence = lastSequence + 1;
        }
      }

      // Format: PNxxxxxx/YY (e.g., PN000001/25)
      const formattedSequence = nextSequence.toString().padStart(6, "0");
      const nextPONumber = `PN${formattedSequence}/${currentYear}`;

      console.log("🆕 Generated next purchase receipt number:", nextPONumber);
      return nextPONumber;
    } catch (error) {
      console.error("❌ Error generating purchase receipt number:", error);
      // Fallback: generate a simple sequential number
      const currentYear = new Date().getFullYear().toString().slice(-2);
      const fallbackPO = `PN000001/${currentYear}`;
      console.log("🔄 Using fallback PO number:", fallbackPO);
      return fallbackPO;
    }
  }

  // Purchase Order Items Management
  async getPurchaseOrderItems(
    purchaseOrderId: number,
    tenantDb?: any,
  ): Promise<PurchaseReceiptItem[]> {
    const database = tenantDb || this.getSafeDatabase("getPurchaseOrderItems");

    try {
      // Try with new column name first
      let items;
      try {
        items = await database
          .select()
          .from(purchaseReceiptItems)
          .where(eq(purchaseReceiptItems.purchaseReceiptId, purchaseOrderId));
      } catch (newColumnError) {
        // If that fails, try with old column name
        console.log("Trying with old column name...");
        items = await database.execute(
          sql`SELECT * FROM purchase_receipt_items WHERE purchase_order_id = ${purchaseOrderId}`,
        );
        items = items.rows || [];
      }

      return items || [];
    } catch (error) {
      console.error(
        `❌ Error fetching purchase order items for order ${purchaseOrderId}:`,
        error,
      );
      return [];
    }
  }

  async addPurchaseOrderItems(
    purchaseOrderId: number,
    items: InsertPurchaseReceiptItem[],
    tenantDb?: any,
  ): Promise<PurchaseReceiptItem[]> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("addPurchaseOrderItems");
      const itemsWithOrderId = items.map((item) => ({
        ...item,
        purchaseReceiptId: purchaseOrderId,
        receivedQuantity: 0, // Initialize to 0
      }));

      const result = await database
        .insert(purchaseReceiptItems)
        .values(itemsWithOrderId)
        .returning();
      return result || [];
    } catch (error) {
      console.error(`❌ Error in addPurchaseOrderItems:`, error);
      throw error;
    }
  }

  async updatePurchaseOrderItem(
    id: number,
    updateData: Partial<InsertPurchaseReceiptItem>,
    tenantDb?: any,
  ): Promise<PurchaseReceiptItem | null> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("updatePurchaseOrderItem");
      const [updatedItem] = await database
        .update(purchaseReceiptItems)
        .set(updateData)
        .where(eq(purchaseReceiptItems.id, id))
        .returning();
      return updatedItem || null;
    } catch (error) {
      console.error(`❌ Error in updatePurchaseOrderItem:`, error);
      throw error;
    }
  }

  async deletePurchaseOrderItem(id: number, tenantDb?: any): Promise<boolean> {
    const database =
      tenantDb || this.getSafeDatabase("deletePurchaseOrderItem");
    try {
      const result = await database
        .delete(purchaseReceiptItems)
        .where(eq(purchaseReceiptItems.id, id));
      return result.rowCount > 0;
    } catch (error) {
      console.error(`❌ Error in deletePurchaseOrderItem:`, error);
      return false;
    }
  }

  async receiveItems(
    purchaseOrderId: number,
    receivedItems: Array<{
      id: number;
      receivedQuantity: number;
      productId?: number;
    }>,
    tenantDb?: any,
  ): Promise<{ success: boolean; status: string }> {
    const database = tenantDb || this.getSafeDatabase("receiveItems");

    // Use transaction for atomicity and consistency
    return await database.transaction(async (tx) => {
      try {
        // Validate and update received quantities for each item
        for (const receivedItem of receivedItems) {
          // Get the current purchase receipt item to validate constraints
          // SECURITY: Ensure item belongs to the specified purchase order
          const currentItemResult = await tx.execute(sql`
            SELECT * FROM purchase_receipt_items 
            WHERE id = ${receivedItem.id} AND purchase_receipt_id = ${purchaseOrderId}
            LIMIT 1
          `);

          const currentItem = currentItemResult.rows[0];

          if (!currentItem) {
            throw new Error(
              `Purchase order item with ID ${receivedItem.id} not found`,
            );
          }

          // Validate received quantity constraints
          if (receivedItem.receivedQuantity < 0) {
            throw new Error(
              `Received quantity cannot be negative for item ${receivedItem.id}`,
            );
          }

          if (receivedItem.receivedQuantity > currentItem.quantity) {
            throw new Error(
              `Received quantity (${receivedItem.receivedQuantity}) cannot exceed ordered quantity (${currentItem.quantity}) for item ${receivedItem.id}`,
            );
          }

          // Update received quantity in purchase_receipt_items
          await tx.execute(sql`
            UPDATE purchase_receipt_items 
            SET received_quantity = ${receivedItem.receivedQuantity}
            WHERE id = ${receivedItem.id}
          `);

          // Update inventory if product exists and inventory tracking is enabled
          const increaseAmount =
            receivedItem.receivedQuantity - (currentItem.receivedQuantity || 0);
          if (currentItem.productId && increaseAmount > 0) {
            const [product] = await tx
              .select()
              .from(products)
              .where(eq(products.id, currentItem.productId))
              .limit(1);

            if (product && product.trackInventory) {
              // Update product stock
              await tx
                .update(products)
                .set({
                  stock: sql`${products.stock} + ${increaseAmount}`,
                })
                .where(eq(products.id, currentItem.productId));

              // Record inventory transaction for audit trail
              await tx.insert(inventoryTransactions).values({
                productId: currentItem.productId,
                type: "purchase_receipt",
                quantity: increaseAmount,
                unitPrice: currentItem.unitPrice,
                totalAmount: (
                  Number(currentItem.unitPrice) * increaseAmount
                ).toFixed(2),
                referenceType: "purchase_order", // This should map to purchase_receipt
                referenceId: purchaseOrderId.toString(),
                employeeId: null, // Could be passed as parameter if needed
                notes: `Received ${increaseAmount} units from PO ${purchaseOrderId}`, // This should reflect purchase receipt
              });
            }
          }
        }

        // Recompute purchase order status based on all items from purchase_receipt_items
        const allItemsResult = await tx.execute(sql`
          SELECT * FROM purchase_receipt_items 
          WHERE purchase_receipt_id = ${purchaseOrderId}
        `);

        const allItems = allItemsResult.rows || [];

        const fullyReceived = allItems.every(
          (item) => item.receivedQuantity >= item.quantity,
        );
        const partiallyReceived = allItems.some(
          (item) => item.receivedQuantity > 0,
        );

        let newStatus = "pending";
        if (fullyReceived) {
          newStatus = "received";
        } else if (partiallyReceived) {
          newStatus = "partially_received";
        }

        // Update purchase order status
        await tx
          .update(purchaseReceipts)
          .set({
            status: newStatus,
            updatedAt: new Date(),
            actualDeliveryDate: fullyReceived ? new Date() : null,
          })
          .where(eq(purchaseReceipts.id, purchaseOrderId));

        return { success: true, status: newStatus };
      } catch (error) {
        console.error(`❌ Error in receiveItems:`, error);
        throw error;
      }
    });
  }

  // Purchase Receipts Management Implementation
  async getPurchaseReceipts(
    options: {
      supplierId?: number;
      status?: string;
      search?: string;
      startDate?: string;
      endDate?: string;
      page?: number;
      limit?: number;
    } = {},
    tenantDb?: any,
  ): Promise<PurchaseReceipt[]> {
    const database = tenantDb || this.getSafeDatabase("getPurchaseReceipts");

    try {
      console.log(`🔍 Fetching purchase receipts with options:`, options);

      // Build query conditions using Drizzle
      const conditions = [];
      const {
        supplierId,
        status,
        search,
        startDate,
        endDate,
        page = 1,
        limit,
      } = options;

      if (supplierId) {
        conditions.push(eq(purchaseReceipts.supplierId, supplierId));
      }

      if (status && status !== "all") {
        conditions.push(eq(purchaseReceipts.status, status));
      }

      if (search) {
        conditions.push(
          or(
            ilike(purchaseReceipts.receiptNumber, `%${search}%`),
            ilike(purchaseReceipts.notes, `%${search}%`),
          ),
        );
      }

      if (startDate && endDate) {
        const start = new Date(startDate);
        const end = new Date(endDate);
        conditions.push(
          and(
            gte(purchaseReceipts.purchaseDate, start),
            lte(purchaseReceipts.purchaseDate, end),
          ),
        );
      }

      // Build the query using Drizzle
      let query = database
        .select({
          id: purchaseReceipts.id,
          receiptNumber: purchaseReceipts.receiptNumber,
          supplierId: purchaseReceipts.supplierId,
          employeeId: purchaseReceipts.employeeId,
          status: purchaseReceipts.status,
          purchaseDate: purchaseReceipts.purchaseDate,
          actualDeliveryDate: purchaseReceipts.actualDeliveryDate,
          subtotal: purchaseReceipts.subtotal,
          tax: purchaseReceipts.tax,
          total: purchaseReceipts.total,
          notes: purchaseReceipts.notes,
          createdAt: purchaseReceipts.createdAt,
          updatedAt: purchaseReceipts.updatedAt,
          supplierName: suppliers.name,
          supplierCode: suppliers.code,
          employeeName: employees.name,
          employeeCode: employees.employeeId,
        })
        .from(purchaseReceipts)
        .leftJoin(suppliers, eq(purchaseReceipts.supplierId, suppliers.id))
        .leftJoin(employees, eq(purchaseReceipts.employeeId, employees.id));

      if (conditions.length > 0) {
        query = query.where(and(...conditions));
      }

      query = query.orderBy(desc(purchaseReceipts.createdAt));

      if (limit) {
        const offset = (page - 1) * limit;
        query = query.limit(limit).offset(offset);
      }

      console.log(`📋 Executing purchase receipts query with Drizzle`);

      // Execute the query
      const receipts = await query;

      console.log(
        `✅ Successfully fetched ${receipts.length} purchase receipts`,
      );
      return receipts as PurchaseReceipt[];
    } catch (error) {
      console.error(`❌ Error in getPurchaseReceipts:`, error);
      return [];
    }
  }

  async createPurchaseReceipt(
    receiptData: InsertPurchaseReceipt,
    items: InsertPurchaseReceiptItem[],
    tenantDb?: any,
  ): Promise<PurchaseReceipt> {
    const database = tenantDb || this.getSafeDatabase("createPurchaseReceipt");

    try {
      console.log("🔍 Creating purchase receipt with data:", receiptData);
      console.log("📦 Processing", items.length, "items");

      // Create the main receipt record
      const [receipt] = await database
        .insert(purchaseReceipts)
        .values(receiptData)
        .returning();

      console.log("✅ Purchase receipt created with ID:", receipt.id);

      // Create receipt items if provided
      if (items && items.length > 0) {
        const itemsToInsert = items.map((item) => ({
          ...item,
          purchaseReceiptId: receipt.id,
        }));

        console.log("📝 Creating", itemsToInsert.length, "receipt items");
        const insertedItems = await database
          .insert(purchaseReceiptItems)
          .values(itemsToInsert)
          .returning();

        console.log("✅", insertedItems.length, "receipt items created");
      }

      return receipt;
    } catch (error) {
      console.error("❌ Error creating purchase receipt:", error);
      throw error;
    }
  }

  async uploadPurchaseReceiptDocument(
    documentData: InsertPurchaseReceiptDocument,
    tenantDb?: any,
  ): Promise<PurchaseReceiptDocument> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("uploadPurchaseReceiptDocument");
      const [uploadedDoc] = await database
        .insert(purchaseReceiptDocuments)
        .values(documentData)
        .returning();
      return uploadedDoc;
    } catch (error) {
      console.error(`❌ Error in uploadPurchaseReceiptDocument:`, error);
      throw error;
    }
  }

  // Purchase Order Documents Management Implementation
  async getPurchaseOrderDocuments(
    purchaseOrderId: number,
    tenantDb?: any,
  ): Promise<PurchaseReceiptDocument[]> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("getPurchaseOrderDocuments");
      const result = await database.query(
        `
        SELECT 
          pod.*
        FROM purchase_receipt_documents pod
        WHERE pod.purchase_receipt_id = $1
        ORDER BY pod.created_at DESC
      `,
        [purchaseOrderId],
      );
      return result.rows || [];
    } catch (error) {
      console.error(`❌ Error in getPurchaseOrderDocuments:`, error);
      return [];
    }
  }

  async uploadPurchaseOrderDocument(
    documentData: InsertPurchaseReceiptDocument,
    tenantDb?: any,
  ): Promise<PurchaseReceiptDocument> {
    try {
      const database =
        tenantDb || this.getSafeDatabase("uploadPurchaseOrderDocument");
      const [uploadedDoc] = await database
        .insert(purchaseReceiptDocuments)
        .values({
          ...documentData,
          purchaseReceiptId: documentData.purchaseReceiptId,
        })
        .returning();
      return uploadedDoc;
    } catch (error) {
      console.error(`❌ Error in uploadPurchaseOrderDocument:`, error);
      throw error;
    }
  }

  async deletePurchaseOrderDocument(
    id: number,
    tenantDb?: any,
  ): Promise<boolean> {
    const database =
      tenantDb || this.getSafeDatabase("deletePurchaseOrderDocument");
    try {
      const result = await database
        .delete(purchaseReceiptDocuments)
        .where(eq(purchaseReceiptDocuments.id, id))
        .returning();
      return result.length > 0;
    } catch (error) {
      console.error("Error deleting purchase order document:", error);
      throw error;
    }
  }

  async getPurchaseOrdersWithDetails(
    options?: any,
    tenantDb?: any,
  ): Promise<any> {
    const database =
      tenantDb || this.getSafeDatabase("getPurchaseOrdersWithDetails");
    console.log(
      "🔍 Storage: getPurchaseOrdersWithDetails called with options:",
      options,
    );

    try {
      // Build base query for purchase orders
      let whereConditions = [];

      if (options?.status && options.status !== "all") {
        whereConditions.push(eq(purchaseReceipts.status, options.status));
      }

      if (options?.supplierId) {
        whereConditions.push(
          eq(purchaseReceipts.supplierId, options.supplierId),
        );
      }

      if (options?.search) {
        whereConditions.push(
          or(
            ilike(purchaseReceipts.receiptNumber, `%${options.search}%`),
            ilike(purchaseReceipts.notes, `%${options.search}%`),
          ),
        );
      }

      if (options?.startDate && options?.endDate) {
        const startDate = new Date(options.startDate);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(options.endDate);
        endDate.setHours(23, 59, 59, 999);

        console.log("🗓️ Date filtering (both dates):", {
          startDate: startDate,
          endDate: endDate,
          originalStart: options.startDate,
          originalEnd: options.endDate,
        });

        whereConditions.push(
          and(
            gte(purchaseReceipts.purchaseDate, startDate),
            lte(purchaseReceipts.purchaseDate, endDate),
          ),
        );
      } else if (options?.startDate) {
        const startDate = new Date(options.startDate);
        startDate.setHours(0, 0, 0, 0);

        console.log("🗓️ Date filtering (start only):", {
          startDate: startDate,
          originalStart: options.startDate,
        });

        whereConditions.push(gte(purchaseReceipts.purchaseDate, startDate));
      } else if (options?.endDate) {
        const endDate = new Date(options.endDate);
        endDate.setHours(23, 59, 59, 999);

        console.log("🗓️ Date filtering (end only):", {
          endDate: endDate,
          originalEnd: options.endDate,
        });

        whereConditions.push(lte(purchaseReceipts.purchaseDate, endDate));
      }

      // Get purchase orders with supplier information
      const ordersQuery = database
        .select({
          // Purchase order fields
          id: purchaseReceipts.id,
          receiptNumber: purchaseReceipts.receiptNumber,
          supplierId: purchaseReceipts.supplierId,
          employeeId: purchaseReceipts.employeeId,
          status: purchaseReceipts.status,
          purchaseDate: purchaseReceipts.purchaseDate,
          actualDeliveryDate: purchaseReceipts.actualDeliveryDate,
          subtotal: purchaseReceipts.subtotal,
          tax: purchaseReceipts.tax,
          total: purchaseReceipts.total,
          notes: purchaseReceipts.notes,
          createdAt: purchaseReceipts.createdAt,
          updatedAt: purchaseReceipts.updatedAt,
          supplierName: suppliers.name,
          supplierCode: suppliers.code,
          supplierPhone: suppliers.phone,
          supplierEmail: suppliers.email,
          // Employee information
          employeeName: employees.name,
          employeeCode: employees.employeeId,
        })
        .from(purchaseReceipts)
        .leftJoin(suppliers, eq(purchaseReceipts.supplierId, suppliers.id))
        .leftJoin(employees, eq(purchaseReceipts.employeeId, employees.id));

      if (whereConditions.length > 0) {
        ordersQuery.where(and(...whereConditions));
      }

      const orders = await ordersQuery.orderBy(
        desc(purchaseReceipts.createdAt),
      );

      console.log(`✅ Storage: Found ${orders.length} purchase orders`);

      // Get items for each purchase order
      const ordersWithDetails = await Promise.all(
        orders.map(async (order) => {
          try {
            // Get purchase receipt items with product information using sql query
            const itemsResult = await database.execute(sql`
              SELECT 
                pri.id,
                pri.purchase_receipt_id as purchaseOrderId,
                pri.product_id as productId,
                pri.product_name as productName,
                pri.sku,
                pri.quantity,
                pri.received_quantity as receivedQuantity,
                pri.unit_price as unitPrice,
                pri.total,
                p.stock as productStock,
                c.name as productCategory
              FROM purchase_receipt_items pri
              LEFT JOIN products p ON pri.product_id = p.id
              LEFT JOIN categories c ON p.category_id = c.id
              WHERE pri.purchase_receipt_id = ${order.id}
              ORDER BY pri.id
            `);

            const items = itemsResult.rows || [];

            // Calculate summary statistics
            const itemCount = items.length;
            const totalQuantity = items.reduce(
              (sum, item) => sum + (item.quantity || 0),
              0,
            );
            const totalReceived = items.reduce(
              (sum, item) => sum + (item.receivedQuantity || 0),
              0,
            );
            const receivedPercentage =
              totalQuantity > 0
                ? Math.round((totalReceived / totalQuantity) * 100)
                : 0;

            return {
              ...order,
              items: items || [],
              itemCount,
              totalQuantity,
              totalReceived,
              receivedPercentage,
              isFullyReceived:
                totalReceived >= totalQuantity && totalQuantity > 0,
              // Format dates for display
              purchaseDateFormatted: order.purchaseDate
                ? new Date(order.purchaseDate).toLocaleDateString("vi-VN")
                : null,
              actualDeliveryDateFormatted: order.actualDeliveryDate
                ? new Date(order.actualDeliveryDate).toLocaleDateString("vi-VN")
                : null,
              createdAtFormatted: order.createdAt
                ? new Date(order.createdAt).toLocaleString("vi-VN")
                : null,
            };
          } catch (itemError) {
            console.error(
              `❌ Error fetching items for purchase order ${order.id}:`,
              itemError,
            );
            return {
              ...order,
              items: [],
              itemCount: 0,
              totalQuantity: 0,
              totalReceived: 0,
              receivedPercentage: 0,
              isFullyReceived: false,
            };
          }
        }),
      );

      console.log(
        `✅ Storage: Successfully fetched ${ordersWithDetails.length} purchase orders with complete details`,
      );

      // Apply pagination if specified
      const page = options?.page || 1;
      const limit = options?.limit;

      if (limit) {
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedOrders = ordersWithDetails.slice(startIndex, endIndex);

        return {
          data: paginatedOrders,
          pagination: {
            currentPage: page,
            totalItems: ordersWithDetails.length,
            totalPages: Math.ceil(ordersWithDetails.length / limit),
            limit: limit,
            hasNext: endIndex < ordersWithDetails.length,
            hasPrev: page > 1,
          },
        };
      }

      return {
        data: ordersWithDetails,
        pagination: {
          currentPage: 1,
          totalItems: ordersWithDetails.length,
          totalPages: 1,
          limit: ordersWithDetails.length,
          hasNext: false,
          hasPrev: false,
        },
      };
    } catch (error) {
      console.error(
        "❌ Storage: Error in getPurchaseOrdersWithDetails:",
        error,
      );
      return {
        data: [],
        pagination: {
          currentPage: 1,
          totalItems: 0,
          totalPages: 0,
          limit: 0,
          hasNext: false,
          hasPrev: false,
        },
      };
    }
  }

  // Income Voucher Methods Implementation
  async getIncomeVouchers(tenantDb?: any): Promise<any[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getIncomeVouchers");
      const result = await database
        .select()
        .from(incomeVouchers)
        .orderBy(desc(incomeVouchers.createdAt));
      return result || [];
    } catch (error) {
      console.error("Error fetching income vouchers:", error);
      return [];
    }
  }

  async createIncomeVoucher(voucherData: any, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createIncomeVoucher");

    const [voucher] = await database
      .insert(incomeVouchers)
      .values({
        voucherNumber: voucherData.voucherNumber,
        date: voucherData.date,
        amount: voucherData.amount,
        account: voucherData.account,
        recipient: voucherData.recipient,
        phone: voucherData.phone || null,
        category: voucherData.category,
        description: voucherData.description || null,
      })
      .returning();

    return voucher;
  }

  async updateIncomeVoucher(
    id: string,
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    try {
      const database = tenantDb || this.getSafeDatabase("updateIncomeVoucher");

      console.log("Updating income voucher:", { id, voucherData });

      const [voucher] = await database
        .update(incomeVouchers)
        .set({
          voucherNumber: voucherData.voucherNumber,
          date: voucherData.date,
          amount: voucherData.amount.toString(), // Ensure amount is string
          account: voucherData.account,
          recipient: voucherData.recipient,
          phone: voucherData.phone || null,
          category: voucherData.category,
          description: voucherData.description || null,
          updatedAt: new Date(),
        })
        .where(eq(incomeVouchers.id, parseInt(id)))
        .returning();

      console.log("Income voucher updated successfully:", voucher);
      return voucher;
    } catch (error) {
      console.error("Error updating income voucher:", error);
      throw error;
    }
  }

  async deleteIncomeVoucher(id: string, tenantDb?: any): Promise<void> {
    const database = tenantDb || this.getSafeDatabase("deleteIncomeVoucher");

    await database
      .delete(incomeVouchers)
      .where(eq(incomeVouchers.id, parseInt(id)));
  }

  // Expense Voucher methods implementation
  async getExpenseVouchers(tenantDb?: any): Promise<any[]> {
    try {
      const database = tenantDb || this.getSafeDatabase("getExpenseVouchers");
      const result = await database
        .select()
        .from(expenseVouchers)
        .orderBy(desc(expenseVouchers.createdAt));
      return result || [];
    } catch (error) {
      console.error("Error fetching expense vouchers:", error);
      return [];
    }
  }

  async createExpenseVoucher(voucherData: any, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createExpenseVoucher");
    try {
      const [voucher] = await database
        .insert(expenseVouchers)
        .values({
          voucherNumber: voucherData.voucherNumber,
          date: voucherData.date,
          amount: voucherData.amount,
          account: voucherData.account,
          recipient: voucherData.recipient,
          phone: voucherData.phone || null,
          category: voucherData.category,
          description: voucherData.description || null,
        })
        .returning();
      return voucher;
    } catch (error) {
      console.error("Error creating expense voucher:", error);
      throw error;
    }
  }

  async updateExpenseVoucher(
    id: string,
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateExpenseVoucher");

    const [voucher] = await database
      .update(expenseVouchers)
      .set({
        ...voucherData,
        updatedAt: new Date(),
      })
      .where(eq(expenseVouchers.id, parseInt(id)))
      .returning();

    return voucher;
  }

  async deleteExpenseVoucher(id: string, tenantDb?: any): Promise<void> {
    const database = tenantDb || this.getSafeDatabase("deleteExpenseVoucher");

    await database
      .delete(expenseVouchers)
      .where(eq(expenseVouchers.id, parseInt(id)));
  }

  async getNextExpenseVoucherSequence(tenantDb?: any): Promise<number> {
    const database = tenantDb || this.getSafeDatabase("getNextExpenseVoucherSequence");

    try {
      // Get the current year for filtering
      const currentYear = new Date().getFullYear();
      const yearSuffix = currentYear.toString().slice(-2);

      console.log(`🔍 Getting next expense voucher sequence for year: ${currentYear} (suffix: ${yearSuffix})`);

      // Find the highest sequence number for the current year using raw SQL
      const result = await database.execute(sql`
        SELECT voucher_number 
        FROM expense_vouchers 
        WHERE voucher_number LIKE ${'PC%/' + yearSuffix}
        ORDER BY 
          CAST(SUBSTRING(voucher_number FROM 3 FOR 6) AS INTEGER) DESC
        LIMIT 1
      `);

      console.log(`📋 Query result:`, result.rows);

      if (!result.rows || result.rows.length === 0) {
        console.log(`✅ No vouchers found for year ${currentYear}, starting from 1`);
        return 1;
      }

      // Extract sequence number from voucher number (PC######/YY)
      const lastVoucherNumber = result.rows[0].voucher_number;
      console.log(`🔍 Last voucher number found: ${lastVoucherNumber}`);

      const match = lastVoucherNumber.match(/^PC(\d{6})\/\d{2}$/);

      if (match) {
        const lastSequence = parseInt(match[1], 10);
        const nextSequence = lastSequence + 1;
        console.log(`✅ Last sequence: ${lastSequence}, Next sequence: ${nextSequence}`);
        return nextSequence;
      } else {
        console.log(`⚠️ Voucher number format doesn't match expected pattern, starting from 1`);
        return 1;
      }
    } catch (error) {
      console.error("❌ Error getting expense voucher sequence:", error);
      // If error occurs, start from 1
      return 1;
    }
  }

  async createExpenseVoucher(voucherData: any, tenantDb?: any): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("createExpenseVoucher");

    const [voucher] = await database
      .insert(expenseVouchers)
      .values({
        voucherNumber: voucherData.voucherNumber,
        date: voucherData.date,
        amount: voucherData.amount,
        account: voucherData.account,
        recipient: voucherData.recipient,
        phone: voucherData.phone || null,
        category: voucherData.category,
        description: voucherData.description || null,
      })
      .returning();

    return voucher;
  }

  async updateExpenseVoucher(
    id: string,
    voucherData: any,
    tenantDb?: any,
  ): Promise<any> {
    const database = tenantDb || this.getSafeDatabase("updateExpenseVoucher");

    const [voucher] = await database
      .update(expenseVouchers)
      .set({
        ...voucherData,
        updatedAt: new Date(),
      })
      .where(eq(expenseVouchers.id, parseInt(id)))
      .returning();

    return voucher;
  }

  async deleteExpenseVoucher(id: string, tenantDb?: any): Promise<void> {
    const database = tenantDb || this.getSafeDatabase("deleteExpenseVoucher");

    await database
      .delete(expenseVouchers)
      .where(eq(expenseVouchers.id, parseInt(id)));
  }
}

export const storage = new DatabaseStorage();